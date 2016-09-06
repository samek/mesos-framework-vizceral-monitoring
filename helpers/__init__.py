# Copyright 2015 (c) Marco Massenzio.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from __future__ import print_function

from abc import ABCMeta, abstractmethod
import json
import logging
import os
import re
import urlparse

import kazoo.client
import kazoo.exceptions

# TODO: this needs to be adjusted with the correct package
#from mesos.messages_pb2 import MasterInfo


__author__ = 'marco@mesosphere.io'


class ZookeeperNoMaster(kazoo.exceptions.ZookeeperError):
    pass


class ZooKeeperInvalidData(kazoo.exceptions.ZookeeperError):
    pass


class Constants(object):
    #: Default URI for a local ZK; uses /mesos as the path.
    DEFAULT_ZK = "zk://localhost:2181/mesos"

    #: timeout for a server connection, in seconds.
    DEFAULT_TIMEOUT_SEC = 10

    #: Default label for master info, used as prefix in the znodes, names (eg, `json.info_000001`).
    MASTER_INFO_LABEL = 'json.info'

    #: Legacy (< 0.24) label for Protocol Buffer stored data.
    MASTER_PB_LABEL = 'info'


class ZookeeperDiscovery(object):
    """ Abstract base class for all ZK discovery implementations.
    """
    __metaclass__ = ABCMeta

    def __init__(self, zk_uri, timeout_sec, label):
        """ Constructs a discovery class.

        :param zk_uri: the ensemble uri, in ```zk://host:port,[host:port,...]/path/to``` format.
        :param timeout_sec: the ZK client timeout (in seconds).
        :param label: the znode's label (eg. `json.info`) which prefixes the sequence nodes.
        """
        self.uri = zk_uri
        self.zk_hosts, self.zk_path = self._parse_uri(self.uri)
        self._zk = kazoo.client.KazooClient(hosts=self.zk_hosts, timeout=timeout_sec)
        self.label = label
        try:
            self._zk.start(timeout=timeout_sec)
        except Exception as e:
            logging.error('Timeout trying to connect to a ZK ensemble [{}]'.format(e))
            raise RuntimeError('No Zookeeper ensemble available at {}'.format(self.zk_hosts))

    @abstractmethod
    def retrieve_leader(self):
        """Retrieves the full Master information for the Leader Master

        :return: the retrieved data for one Master node
        :rtype: dict

        :raises NoNodeError: if the ```self.zk_path`` does not exist
        :raises ZookeeperExcetpion: if there are errors while fetching the data
        :raises ParseError: if the data cannot be parsed into a valid ```MasterInfo``` protobuf
        """
        pass

    @abstractmethod
    def retrieve_all(self):
        """ Retrieves information about the entire set of Mesos Masters currently active

        :return: the retrieved data for all the Master nodes, in sequence order.
        :rtype: list(dict)

        :raises NoNodeError: if the ```self.zk_path`` does not exist
        :raises ZookeeperExcetpion: if there are errors while fetching the data
        :raises ParseError: if the data cannot be parsed into a valid ```MasterInfo``` protobuf
        """
        pass

    @abstractmethod
    def get_leader_ip(self):
        """ Most basic info about the elected Leader, the (IP, port) tuple.

        :return: the (ip, port) tuple for the Leading Master
        :rtype: tuple(string, int)
        """
        pass

    @staticmethod
    def _parse_uri(uri):
        parse_url = urlparse.urlparse(uri, scheme='zk:', allow_fragments=False)
        return parse_url.netloc, parse_url.path

    def _get_nodes(self):
        """ Gets all the znode names that are currently active

            Internal method, should **not** be used by clients; use either of ```retrieve_all()```
            or ```retrieve_leader()```.

        :return: the list of node names (**not** the full path)
        :rtype: list(str)

        :raises NoNodeError: if the ```self.zk_path`` does not exist
        :raises ZookeeperExcetpion: if there are errors while fetching the data
        :raises ParseError: if the data cannot be parsed into a valid ```MasterInfo``` protobuf
        """
        if not self._zk.exists(self.zk_path):
            raise kazoo.exceptions.NoNodeError(
                'The path {self.zk_path} provided in the URI {self.uri} '
                'could not be found on the Zookeeper ensemble'.format(self=self))
        nodes = [node for node in self._zk.get_children(self.zk_path) if
                 node.startswith(self.label)]
        if not nodes:
            raise ZookeeperNoMaster('No Mesos Master found for {self.uri}'.format(self=self))
        return nodes

    def get_master_url(self):
        """ Convenience method to retrieve the Leader location as a URL (``http://hostname:port``)
            from the ZK data

            This method is safe to call (will not raise) but may return an empty (``None``) URL

        :return: the leading Mesos Master's URL, as retrieved from the ZK ensemble
        :rtype: str
        """
        try:
            ip, port = self.get_leader_ip()
            return 'http://{host}:{port}'.format(host=ip, port=port)
        except kazoo.exceptions.ZookeeperError:
            return None

    def get_all_urls(self):
        """ Gets all Masters' URLs

        :return: the list of hostname:port URLs for all the masters; or an empty list if an error
                 occurs
        :rtype: list(str)
        """
        urls = []
        try:
            masters_info = self.retrieve_all()
            for master_info in masters_info:
                urls.append(
                    'http://{host}:{port}'.format(host=master_info['hostname'],
                                                  port=master_info['port']))
        except kazoo.exceptions.ZookeeperError:
            # TODO: add logging and log(ERROR) here
            pass
        return urls


class ZookeeperDiscoveryProtocolBuffer(ZookeeperDiscovery):
    """ Simple Reference Implementation (RI) for Mesos Master ZK discovery.

        This class takes a zookeeper URI (complete with path) and obtains the full
        address(es) of the Mesos Masters registered with Zookeeper.
    """

    #: RegEx to parse a Master ID of the form "master@192.168.1.51:5050"
    ID_REGEX = re.compile(r'(?P<name>\w+)@(?P<ip>\d+\.\d+\.\d+.\d+):(?P<port>\d+)')

    def __init__(self, zk_uri=Constants.DEFAULT_ZK, timeout_sec=Constants.DEFAULT_TIMEOUT_SEC):
        super(ZookeeperDiscoveryProtocolBuffer, self).__init__(zk_uri, timeout_sec,
                                                               label=Constants.MASTER_PB_LABEL)

    def get_leader_ip(self):
        leader = self.retrieve_leader()
        return leader.get('ip', 'localhost'), leader.get('port', 5050)

    def retrieve_leader(self):
        """ Retrieves from ZK the MasterInfo data.

        :return: the retrieved ```MasterInfo``` data for one Master node
        :rtype: dict

        :raises NoNodeError: if the ```self.zk_path`` does not exist
        :raises ZookeeperExcetpion: if there are errors while fetching the data
        :raises ParseError: if the data cannot be parsed into a valid ```MasterInfo``` protobuf
        """
        all_znodes = self._get_nodes()
        # The lowest numbered znode is the Leader
        all_znodes.sort()
        node_path = os.path.join(self.zk_path, all_znodes[0])
        # TODO: make the ``stat`` accessible to clients of this class, perhaps?
        data, stat = self._zk.get(node_path)
        master_info = MasterInfo()
        master_info.ParseFromString(data)
        return ZookeeperDiscoveryProtocolBuffer._to_dict(master_info)

    def retrieve_all(self):
        """ Retrieves information about the entire set of Mesos Masters currently active


        :return: the retrieved ```MasterInfo``` data for all the Master nodes
        :rtype: list(dict)

        :raises NoNodeError: if the ```self.zk_path`` does not exist
        :raises ZookeeperExcetpion: if there are errors while fetching the data
        :raises ParseError: if the data cannot be parsed into a valid ```MasterInfo``` protobuf
        """
        nodes = self._get_nodes()
        if not nodes:
            raise ZookeeperNoMaster("No Mesos Masters found for {}".format(self.zk_path))
        master_info = []
        for node in nodes:
            node_path = os.path.join(self.zk_path, node)
            data, stat = self._zk.get(node_path)
            master_info_pb = MasterInfo()
            master_info_pb.ParseFromString(data)
            master_info.append(ZookeeperDiscoveryProtocolBuffer._to_dict(master_info_pb))
        return master_info

    @staticmethod
    def _to_dict(pb_info):
        """ Converts a MasterInfo protobuf into its dictionary equivalent

        :param pb_info: MasterInfo
        :return: the equivalent dict
        :rtype: dict
        """
        res = {
            'pid': pb_info.pid,
            'ip': ZookeeperDiscoveryProtocolBuffer._parse_pid(pb_info.pid)[0],
            'hostname': pb_info.hostname,
            'id': pb_info.id,
            'port': pb_info.port,
            'version': pb_info.version
        }
        return res

    @classmethod
    def _parse_pid(cls, pid):
        """ Parse the Master's PID into its components.

        :param pid: a ```MasterInfo``` PID of the form "master@192.168.1.51:5050"
        :return: a tuple of (ip, port, name)
        :rtype: tuple
        """
        match = re.match(cls.ID_REGEX, pid)
        if match:
            return match.group('ip'), int(match.group('port')), match.group('name')
        else:
            raise ValueError("Cannot parse {} into a valid Mesos Master PID".format(pid))


class ZookeeperDiscoveryJson(ZookeeperDiscovery):
    """ Simple Reference Implementation (RI) for Mesos Master ZK discovery.

        This class takes a zookeeper URI (complete with path) and obtains the full
        address(es) of the Mesos Masters registered with Zookeeper.

        This class assumes that the data will be stored in JSON format by the Mesos
        Master nodes (true as of Mesos 0.24.x).
    """

    def __init__(self, zk_uri=Constants.DEFAULT_ZK, timeout_sec=Constants.DEFAULT_TIMEOUT_SEC):
        super(ZookeeperDiscoveryJson, self).__init__(zk_uri, timeout_sec,
                                                     label=Constants.MASTER_INFO_LABEL)

    def get_leader_ip(self):
        leader = self.retrieve_leader()
        address = leader.get('address')
        if not address:
            raise ZooKeeperInvalidData("No Address information in MasterInfo for {}".format(
                leader.get('pid')
            ))
        return address.get('ip'), address.get('port')

    def retrieve_leader(self):
        """ Retrieves from ZK the MasterInfo data.

        :return: the retrieved ```MasterInfo``` data for one Master node
        :rtype: dict

        :raises NoNodeError: if the ```self.zk_path`` does not exist
        :raises ZookeeperExcetpion: if there are errors while fetching the data
        :raises ParseError: if the data cannot be parsed into a valid ```MasterInfo``` protobuf
        """
        all_znodes = self._get_nodes()
        # The lowest numbered znode is the Leader
        all_znodes.sort()
        node_path = os.path.join(self.zk_path, all_znodes[0])
        # TODO: make the ``stat`` accessible to clients of this class, perhaps?
        data, stat = self._zk.get(node_path)
        return json.loads(data)

    def retrieve_all(self):
        """ Retrieves information about the entire set of Mesos Masters currently active


        :return: the retrieved ```MasterInfo``` data for all the Master nodes
        :rtype: list(dict)

        :raises NoNodeError: if the ```self.zk_path`` does not exist
        :raises ZookeeperExcetpion: if there are errors while fetching the data
        :raises ParseError: if the data cannot be parsed into a valid ```MasterInfo``` protobuf
        """
        nodes = self._get_nodes()
        if not nodes:
            raise ZookeeperNoMaster("No Mesos Masters found for {}".format(self.zk_path))
        master_info = []
        for node in nodes:
            node_path = os.path.join(self.zk_path, node)
            data, stat = self._zk.get(node_path)
            master_info.append(json.loads(data))
        return master_info


def zookeeper_discovery(version, zk_uri, timeout_sec=Constants.DEFAULT_TIMEOUT_SEC):
    """ Factory method for a ```ZookeeperDiscovery``` implementation.

    :param version: semantic version (```major.minor.patch```) to choose which implementation
            of ```ZookeeperDiscovery``` to return
    :type version: str
    :return: a subclass of ```ZookeeperDiscovery``` suitable for the given ```version```
    :rtype: ZookeeperDiscovery
    """
    if not re.match(r'\d+\.\d+(\.\d+)?', version):
        raise ValueError("{} is not a valid semantic version for Apache Mesos".format(version))
    try:
        major, minor, _ = tuple(int(x) for x in version.split('.'))
    except ValueError:
        major, minor = tuple(int(x) for x in version.split('.'))
    if major > 0 or minor > 23:
        return ZookeeperDiscoveryJson(zk_uri, timeout_sec)
    else:
        return ZookeeperDiscoveryProtocolBuffer(zk_uri, timeout_sec)
