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

    @staticmethod
    def redirect(keep_original: bool = True):
        sys.stdout = CockpitLogging('stdout', keep_original=keep_original, message=False)
        sys.stderr = CockpitLogging('stderr', keep_original=keep_original, message=False)
        CockpitLogging.message('stdout and stderr', keep_original)

    @staticmethod
    def message(type: str, keep_original):
        _message = f'Cockpit logging initialised for {type}. '
        if keep_original:
            _message += f'Original logging also active. To suppress set keep_original = False.'
        else:
            _message += f'Original logging suppressed.'
        CockpitLogging.get_log_file().write(_message + '\n')

    def __init__(self, type: str, keep_original = True, message = True):
        assert type in ['stdout', 'stderr'], "type must be either 'stdout' or 'stderr'"
        self.type = type
        self.keep_original = keep_original
        # by getting the log file here, we ensure that it is created and ready to be written to
        CockpitLogging.get_log_file()

        if message:
            CockpitLogging.message(self.type, self.keep_original)

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
        
        cls.log_file.close()
        cls.log_file = None
        sys.stdout = sys.__stdout__
        sys.stderr = sys.__stderr__

        print('stdout and stderr logging reset to standard.')
