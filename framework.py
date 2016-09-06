import logging
import uuid
import time
import json
import urllib2


from mesos.interface import Scheduler, mesos_pb2
from mesos.native import MesosSchedulerDriver
from helpers import zookeeper_discovery, ZookeeperNoMaster


logging.basicConfig(level=logging.INFO)


def get_task_log(offer,task):
    task_log={}
    task_log['slave_id']=offer.slave_id
    task_log['task_name']=task.name
    task_log['slave_ip']=offer.hostname
    return task_log


def get_task_log_param(name,slave_id):
    task_log={}
    task_log['slave_id']=mesos_pb2.SlaveID()
    task_log['slave_id'].value= slave_id
    task_log['task_name']=name
    task_log['slave_ip']=''
    return task_log



def new_task(offer):
    task = mesos_pb2.TaskInfo()
    task.task_id.value = str(uuid.uuid4())
    task.slave_id.value = offer.slave_id.value
    task.name = "HelloWorld"

    cpus = task.resources.add()
    cpus.name = "cpus"
    cpus.type = mesos_pb2.Value.SCALAR
    cpus.scalar.value = 0.1

    mem = task.resources.add()
    mem.name = "mem"
    mem.type = mesos_pb2.Value.SCALAR
    mem.scalar.value = 10

    return task


def get_master_status_by_host(hostname):
    return _get_master_status(hostname)

def get_master_status_by_masterinfo(master_info):
    return _get_master_status(master_info.hostname)

def _get_master_status(host):
    req = urllib2.Request('http://' + host + ":5050/state.json")

    req.add_header('Content-type', 'application/json')
    res = urllib2.urlopen(req, timeout=60)
    return json.loads(res.read())

class VizCeralScheduler(Scheduler):

    RUNNING_TASKS=[]
    master_status={}

    def is_running_on_agent(self,slave_id,taskname=None):

        for task in self.RUNNING_TASKS:
            #print str(task['slave_id'])+" vs "+slave_id
            if task['slave_id']== slave_id:
                return True
            else:
                return False

    def remove_from_running_list(self,slave_id):
        for task in self.RUNNING_TASKS:
            if task['slave_id'] == slave_id:
                self.RUNNING_TASKS.remove(task)


    def recon_tasks(self):
        local_running_tasks=[]
        if 'frameworks' in self.master_status:
            for framework in self.master_status['frameworks']:
                for task in framework['tasks']:
                    local_running_tasks.append(get_task_log_param(task['name'],task['slave_id']))

        self.RUNNING_TASKS = local_running_tasks

    def registered(self, driver, framework_id, master_info):
        logging.info("Registered with framework id: %s on: %s",
                     framework_id, master_info.hostname)
        ##TODO get state.json from mesos to see
        self.master_status = get_master_status_by_masterinfo(master_info)
        print json.dumps(self.master_status)
        self.recon_tasks()
        logging.info("Recon tasks done")
        print(self.RUNNING_TASKS)


    def resourceOffers(self, driver, offers):
        logging.info("Recieved resource offers: %s",
                     [o.id.value for o in offers])
        # whenever we get an offer
        # we accept it and use it to launch a task that
        # just echos hello world to stdout
        for offer in offers:
            task = new_task(offer)
            task.command.value = "echo hello world && sleep 120"
            time.sleep(5)
            if self.is_running_on_agent(offer.slave_id):

                logging.info("declining task since it's allready in %s "
                             "using offer %s.",
                             task.task_id.value,
                             offer.id.value)
                driver.declineOffer(offer.id)
            else:

                #self.TASKS.append()
                logging.info("Launching task %s "
                         "using offer %s.",
                         task.task_id.value,
                         offer.id.value)
                #driver.declineOffer(offer.id)
                print offer.hostname
                driver.launchTasks(offer.id, [task])

                self.RUNNING_TASKS.append(get_task_log(offer,task))

    def statusUpdate(self, driver, update):
        '''
        when a task is started, over,
        killed or lost (slave crash, ....), this method
        will be triggered with a status message.
        '''
        #print update
        logging.info("Task %s is in state %s" %
                     (update.task_id.value,
                      mesos_pb2.TaskState.Name(update.state)))
        if update.state != mesos_pb2.TASK_RUNNING and\
           update.state != mesos_pb2.TASK_STAGING and\
           update.state != mesos_pb2.TASK_STARTING:
            ## then we remove it##
            self.remove_from_running_list(update.slave_id)
            logging.info("Removing %s" % update.task_id)



if __name__ == '__main__':
    ZK_HOSTS='zk://172.17.0.4:2181/mesos'

    zk_discovery = zookeeper_discovery('1.0.0', ZK_HOSTS, timeout_sec=15)
    logging.info("Connected")
    reuse_framework_id=""
    try:
        res = zk_discovery.retrieve_leader()
        logging.info("Found leader: {}".format(res.get('hostname')))
        print(json.dumps(res, indent=4, separators=[',', ':']))
        ##print res
        status = get_master_status_by_host(res.get('hostname'))
        if 'frameworks' in status:
            for framework in status['frameworks']:
                if framework['name']=='vizceral':
                    print "NASEL FRAMEWORK"
                    reuse_framework_id = framework['id']
                    #exit(1)

        #exit(1)
    except ZookeeperNoMaster as ex:
        logging.error("Could not find any Mesos Master: {}".format(ex.message))
        exit(1)

    framework = mesos_pb2.FrameworkInfo()
    framework.user = "root"  # Have Mesos fill in the current user.
    framework.name = "vizceral"
    framework.failover_timeout=60

    #print framework

    if reuse_framework_id!="":
        mesos_framework = mesos_pb2.FrameworkID()
        mesos_framework.value=reuse_framework_id
        framework.id.MergeFrom(mesos_framework)
        logging.error("re-registring existing framework "+reuse_framework_id)

    driver = MesosSchedulerDriver(
        VizCeralScheduler(),
        framework,
        ZK_HOSTS

    )

    driver.run()
