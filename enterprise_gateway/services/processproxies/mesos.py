# Copyright (c) Jupyter Development Team.
# Distributed under the terms of the Modified BSD License.
"""Code related to managing kernels running in Mesos clusters."""

import os
import signal
import logging

from jupyter_client import launch_kernel, localinterfaces

from enterprise_gateway.util.mesos.http import Resource
from .processproxy import RemoteProcessProxy

# Default logging level of underlying connectionpool produce too much noise - raise to warning only.
logging.getLogger('urllib3.connectionpool').setLevel(os.environ.get('EG_MESOS_LOG_LEVEL', logging.WARNING))

local_ip = localinterfaces.public_ips()[0]
poll_interval = float(os.getenv('EG_POLL_INTERVAL', '0.5'))
max_poll_attempts = int(os.getenv('EG_MAX_POLL_ATTEMPTS', '10'))
mesos_shutdown_wait_time = float(os.getenv('EG_MESOS_SHUTDOWN_WAIT_TIME', '15.0'))


class MesosClusterProcessProxy(RemoteProcessProxy):
    """Kernel lifecycle management for Mesos clusters."""
    initial_states = {'NEW', 'SUBMITTED', 'ACCEPTED', 'RUNNING'}
    final_states = {'FINISHED', 'KILLED'}  # Don't include FAILED state

    def __init__(self, kernel_manager, proxy_config):
        super(MesosClusterProcessProxy, self).__init__(kernel_manager, proxy_config)
        self.application_id = None
        self.mesos_endpoint \
            = proxy_config.get('mesos_endpoint',
                               kernel_manager.parent.parent.mesos_endpoint)

        # Mesos applications tend to take longer than the default 5 second wait time.  Rather than
        # require a command-line option for those using Mesos, we'll adjust based on a local env that
        # defaults to 15 seconds.  Note: we'll only adjust if the current wait time is shorter than
        # the desired value.
        if kernel_manager.shutdown_wait_time < mesos_shutdown_wait_time:
            kernel_manager.shutdown_wait_time = mesos_shutdown_wait_time
            self.log.debug("{class_name} shutdown wait time adjusted to {wait_time} seconds.".
                           format(class_name=type(self).__name__, wait_time=kernel_manager.shutdown_wait_time))

        # TODO - think through secure mesos connectivity
        self.mesos_client = Resource(url=self.mesos_endpoint)

    def launch_process(self, kernel_cmd, **kwargs):
        """Launches the specified process within a Mesos cluster environment."""
        super(MesosClusterProcessProxy, self).launch_process(kernel_cmd, **kwargs)

        # launch the local run.sh - which is configured for mesos cluster...
        self.local_proc = launch_kernel(kernel_cmd, **kwargs)
        self.pid = self.local_proc.pid
        self.ip = local_ip

        self.log.debug("Mesos cluster kernel launched using Mesos endpoint: {}, pid: {}, Kernel ID: {}, cmd: '{}'"
                       .format(self.mesos_endpoint, self.local_proc.pid, self.kernel_id, kernel_cmd))
        self.confirm_remote_startup()

        return self

    def poll(self):
        """Submitting a new kernel/app to Mesos will take a while to be ACCEPTED.
        Thus application ID will probably not be available immediately for poll.
        So will regard the application as RUNNING when application ID still in ACCEPTED or SUBMITTED state.

        :return: None if the application's ID is available and state is ACCEPTED/SUBMITTED/RUNNING. Otherwise False.
        """
        return None

    def send_signal(self, signum):
        """Currently only support 0 as poll and other as kill.

        :param signum
        :return:
        """
        self.log.debug("MesosClusterProcessProxy.send_signal {}".format(signum))
        if signum == 0:
            return self.poll()
        elif signum == signal.SIGKILL:
            return self.kill()
        else:
            # Mesos api doesn't support the equivalent to interrupts, so take our chances
            # via a remote signal.  Note that this condition cannot check against the
            # signum value because alternate interrupt signals might be in play.
            return super(MesosClusterProcessProxy, self).send_signal(signum)

    def kill(self):
        """Kill a kernel.
        :return: None if the application existed and is not in RUNNING state, False otherwise.
        """
        return None

    def cleanup(self):
        """"""
        # we might have a defunct process (if using waitAppCompletion = false) - so poll, kill, wait when we have
        # a local_proc.
        if self.local_proc:
            self.log.debug("MesosClusterProcessProxy.cleanup: Clearing possible defunct process, pid={}...".
                           format(self.local_proc.pid))
            if super(MesosClusterProcessProxy, self).poll():
                super(MesosClusterProcessProxy, self).kill()
            super(MesosClusterProcessProxy, self).wait()
            self.local_proc = None

        # reset application id to force new query - handles kernel restarts/interrupts
        self.application_id = None

        # for cleanup, we should call the superclass last
        super(MesosClusterProcessProxy, self).cleanup()

    def confirm_remote_startup(self):
        """ Confirms the mesos application is in a started state before returning.  Should post-RUNNING states be
            unexpectedly encountered (FINISHED, KILLED) then we must throw, otherwise the rest of the gateway will
            believe its talking to a valid kernel.
        """
        pass

    def handle_timeout(self):
        """Checks to see if the kernel launch timeout has been exceeded while awaiting connection info."""
        pass

    def get_process_info(self):
        """Captures the base information necessary for kernel persistence relative to Mesos clusters."""
        process_info = super(MesosClusterProcessProxy, self).get_process_info()
        process_info.update({'application_id': self.application_id})
        return process_info

    def load_process_info(self, process_info):
        """Loads the base information necessary for kernel persistence relative to Mesos clusters."""
        super(MesosClusterProcessProxy, self).load_process_info(process_info)
        self.application_id = process_info['application_id']
