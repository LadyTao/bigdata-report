import json
from datetime import datetime, timezone


def get_productline(_):
    if _ == 'Video Editor':
        return 'Filmora'
    elif _ == 'Selfie':
        return 'Effect Store'
    return 'Missing'

def get_license_type(_):
    if _ in [None, ""]:
        return 'Missing'
    return _[:_.find("(")]
    
def get_subscribed_es(_):
    if _ == 1:
        return 'Yes'
    return 'No'

def get_effects_paid(_):
    if _ == 1:
        return 'Yes'
    return 'No'

def get_edm_subscribe_status(_):
    if _ == 1:
        return '已订阅'
    return '未订阅'
    
def get_license(_):
    if _ == 1:
        return "Yes"
    elif _ == 2:
        return "No"
    return "Error"
    
def get_download_free_es(_):
    if _ == 1:
        return "Yes"
    elif _ == 2:
        return "No"
    return "Error"
    
def get_register_platform(_):
    
    register_platform_map = {      
        '1' : 'Filmora Mac',
        '2' : 'Filmora Windows',
        '3' : 'Effect Store',
        '4' : 'Filmora IO',
        '5' : 'Filmora GO',
        '6' : 'Filmora Screen',
        '7' : 'Filmora GO Android',
        '8' : 'Filmora GO IOS',
        '9' : 'Vlogit Android',
        '10' : 'Vlogit IOS'
    }
    if _ in register_platform_map:
        return register_platform_map[_]
    return "Missing"

def get_activated_platform(_):
    if _ in [None, ""]:
        return "Missing"

    _json = json.loads(_)
    """
    {
        'android_go': 0,
        'android_vlog': 0,
        'fxs': 0,
        'go': 0,
        'io': 0,
        'ios_go': 0,
        'ios_vlog': 0,
        'mac': 0,
        'scr': 0,
        'win': 1,
    }
    """
    #pp(_json)
    platform_list = []
    short_long_map = {
        "win": "Filmora Windows",
        "mac": "Filmora Mac",
        "fxs": "Effect Store",
        "io" : "Filmora IO",
        "go" : "Filmora GO",
        "src": "Filmora Screen",
        "android_go": "Filmora GO Android",
        "ios_go": "Filmora GO IOS",
        "android_vlog": "Vlogit Android",
        "ios_vlog": "Vlogit IOS"
    }
    for platform_short, key in _json.items():
        if int(key) == 1:
            #platform_list.append(short_long_map[platform_short])
            return short_long_map[platform_short]
    return "Missing"

def get_language(_):
    lang_map = {
        1: 'English',
        2: 'French',
        3: 'Japanese',
        4: 'Germany',
        5: 'Spanish',
        6: 'Portuguese',
        7: 'Dutch',
        8: 'Russian',
        9: 'Italian',
       10: 'Arabic',
       11: 'Chinese',
       12: 'China Taiwan(zh_tw)',
       13: 'China HongKong(zh_hk)'
    }
    if _ not in lang_map:
        return "Missing"
    return lang_map[_]

def get_es_time(_):
    if _ in ['', None]:
        return datetime.fromtimestamp(int(0))
    return datetime.fromtimestamp(int(_))

def get_productversion(_):
    if _ is None:
        return 'Missing'
    return _


def get_is_old_member(_):
    if _ == 1:
        return 'Yes'
    return 'No'

def get_from_brand(_):
    if _ == 1:
        return 'Wondershare'
    if _ == 2:
        return 'iSkysoft'
    return '其他'


def get_country_cn(_):
    if _ in ["", None]:
        return u"其他"
    return _

field_convert_map = {
    'download_free_es': get_download_free_es,
    'effects_paid': get_effects_paid,
    'subscribed_es': get_subscribed_es,

    'register_platform': get_register_platform,
    'language': get_language,
    'register_time': get_es_time,
    'activated_platform': get_activated_platform,
    'inputtime': get_es_time,
    'last_visit': get_es_time,

    'license': get_license,
    'edm_subscribe_status': get_edm_subscribe_status,
    'is_old_member': get_is_old_member,
    'from_brand':get_from_brand,
    'country_cn': get_country_cn
}

convert_float_fields = [
]

convert_string_field = [
    'email', 'username'
]

order_fields = [
    'id', 
    'uid', 
    'email', 
    'username', 
    'language', 
    'register_platform', 
    'register_time',  
    'activated_platform', 
    'inputtime', 
    'last_visit', 
    'license', 

    'effects_paid',
    'subscribed_es',
    'download_free_es',   
 
    'edm_subscribe_status', 
    'is_old_member', 
    'from_brand',
    'country_cn'
]
