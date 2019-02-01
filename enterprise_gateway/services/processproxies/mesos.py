
# Copyright (c) Jupyter Development Team.
# Distributed under the terms of the Modified BSD License.
"""Code related to managing kernels running in Mesos clusters."""

from jupyter_client import launch_kernel, localinterfaces

from .processproxy import RemoteProcessProxy
from ..util.mesos.http import Resource

try:
    from urlparse import urlparse
except ImportError:
    from urllib.parse import urlparse

local_ip = localinterfaces.public_ips()[0]

class MesosClusterProcessProxy(RemoteProcessProxy):
    """Kernel lifecycle management for Mesos clusters."""

     # Not sure how to get this level out of Mesos yet, will use simplified version for now
    # initial_states = {'NEW', 'SUBMITTED', 'ACCEPTED', 'RUNNING'}
    # final_states = {'FINISHED', 'KILLED'}  # Don't include FAILED state

    initial_states = {'ACTIVE'}
    final_states = {'COMPLETED'}

    def __init__(self, kernel_manager, proxy_config):
        super(MesosClusterProcessProxy, self).__init__(kernel_manager, proxy_config)
        self.application_id = None
        self.mesos_endpoint \
            = proxy_config.get('mesos_endpoint',
                               kernel_manager.parent.parent.mesos_endpoint)
        mesos_master = urlparse(self.mesos_endpoint).hostname
        self.mesos_client = Resource(address=mesos_master)

        return

    def launch_process(self, kernel_cmd, **kwargs):
        """Launches the specified process within a mesos cluster environment."""
        super(MesosClusterProcessProxy, self).launch_process(kernel_cmd, **kwargs)

        print("launching the mesos kernel")

        # launch the local run.sh - which is configured for mesos-cluster...
        self.local_proc = launch_kernel(kernel_cmd, **kwargs)
        self.pid = self.local_proc.pid
        self.ip = local_ip

        self.log.debug("Mesos cluster kernel launched using Mesos endpoint: {}, pid: {}, Kernel ID: {}, cmd: '{}'"
                       .format(self.mesos_endpoint, self.local_proc.pid, self.kernel_id, kernel_cmd))
        self.confirm_remote_startup()

        return self

    # def poll(self):
    #     return
    #
    # def wait(self):
    #     return
    #
    # def send_signal(self, signum):
    #     return
    #
    # def kill(self):
    #     return

    def confirm_remote_startup(self, kernel_cmd, **kw):
        """ Confirms the Mesos application is in a started state before returning.  Should post-RUNNING states be
            unexpectedly encountered (FINISHED, KILLED) then we must throw, otherwise the rest of the gateway will
            believe its talking to a valid kernel.
        """
        self.start_time = RemoteProcessProxy.get_current_time()
        i = 0
        ready_to_connect = False  # we're ready to connect when we have a connection file to use
        # while not ready_to_connect:
        #     i += 1
        #     self.handle_timeout()
        #
        #     ## TODO: get application id
        #     if self._get_application_id(True):
        #         # Once we have an application ID, start monitoring state, obtain assigned host and get connection info
        #         app_state = self.mesos_client.get_framework_state(self.application_id)
        #
        #         if app_state in MesosClusterProcessProxy.final_states:
        #             error_message = "KernelID: '{}', ApplicationID: '{}' unexpectedly found in " \
        #                                              "state '{}' during kernel startup!".\
        #                             format(self.kernel_id, self.application_id, app_state)
        #             self.log_and_raise(http_status_code=500, reason=error_message)
        #
        #         self.log.debug("{}: State: '{}', Host: '{}', KernelID: '{}', ApplicationID: '{}'".
        #                        format(i, app_state, self.assigned_host, self.kernel_id, self.application_id))
        #     else:
        #         self.detect_launch_failure()
        return



    def handle_timeout(self):
        print("TIMED OUT!!!!!")
        return

