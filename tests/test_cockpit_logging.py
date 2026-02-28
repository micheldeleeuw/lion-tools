import pytest
import sys
import time
from lion_tools import CockpitLogging

# sys.stdout = sys.__stdout__
# sys.stderr = sys.__stderr__

def test_cockpit_logging():

    with pytest.raises(Exception):
        CockpitLogging.reset()

    CockpitLogging.redirect()

    print('This is a test log message to both cockpit and original logging.')

    sys.stdout = CockpitLogging('stdout', keep_original=False)
    sys.stderr = CockpitLogging('stderr', keep_original=False)

    print('This is a test log message only to cockpit logging.')

    CockpitLogging.reset()

    from lion_tools import settings
    print(settings.LION_TOOLS_COCKPIT_PATH)

def test_cockpit_logging_reset():
    iterations = 20 
    CockpitLogging.redirect()

    for i in range(iterations):
        print(f'The time is now {time.time()}')
        time.sleep(1)

    CockpitLogging.reset()
