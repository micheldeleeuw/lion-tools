import sys
import os
from .settings import LION_TOOLS_COCKPIT_PATH
from .settings import cleanup_old_files

class CockpitLogging:

    log_file = None
    standard_loggers = None

    @classmethod
    def get_log_file(cls):
        if cls.log_file:
            return cls.log_file
        else:
            # keep old stdout and stderr to be able to reset cockpit logging
            if not cls.standard_loggers:
                cls.standard_loggers = dict(stdout=sys.stdout, stderr=sys.stderr)

            cleanup_old_files()
            
            log_path = str(LION_TOOLS_COCKPIT_PATH.joinpath('_lion_tools_tmp_logging_pid_' + str(os.getpid()) + ".log"))
            cls.log_file = open(log_path, "w", encoding="utf-8")
            print(f'Log file {log_path} created.')
            return cls.log_file

    def __init__(self, type: str | list[str] = ['stdout', 'stderr'], keep_original = True):
        type = [type] if isinstance(type, str) else type
        assert all(t in ['stdout', 'stderr'] for t in type), "type must be either 'stdout' or 'stderr'"
        self.type = type
        self.keep_original = keep_original
        if self.keep_original:
            self.write(f'External logging initialised for {self.type}. To supress original logging, set keep_original to False.\n')
        else:
            self.write(f'External logging initialised for {self.type}. Original logging suppressed.\n')

    def write(self, data):
        CockpitLogging.get_log_file().write(data)
        CockpitLogging.get_log_file().flush()

        if self.keep_original:
            CockpitLogging.standard_loggers[self.type].write(data)
    
    def flush(self):
        CockpitLogging.get_log_file().flush()
        if self.keep_original:
            CockpitLogging.standard_loggers[self.type].flush()

    @classmethod
    def reset(cls):
        if not cls.log_file:
            raise Exception('Lion Tools logging not active, reset not possible.')
        
        print('Resetting logging to standard.')

        # cls.log_file.close()
        cls.log_file = None
        sys.stdout = cls.standard_loggers['stdout']
        sys.stderr = cls.standard_loggers['stderr']

        print('Logging reset to standard.')



