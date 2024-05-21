import sys, requests

def get_platform():
    # get platform name
    # hack to tell if using on network machine - windows implies on network
    if sys.platform.lower().startswith('win'):
        verify = False
        operating_system = 'windows'
        requests.packages.urllib3.disable_warnings()
    else:
        verify = True
        operating_system = 'not windows'

    return verify, operating_system

verify, operating_system = get_platform()