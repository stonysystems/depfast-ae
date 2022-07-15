import os
from simplerpc.marshal import Marshal
from simplerpc.future import Future

class ExampleClientService(object):
    HELLO = 0x177f6de2

    __input_type_info__ = {
        'hello': ['std::vector<int32_t>'],
    }

    __output_type_info__ = {
        'hello': [],
    }

    def __bind_helper__(self, func):
        def f(*args):
            return getattr(self, func.__name__)(*args)
        return f

    def __reg_to__(self, server):
        server.__reg_func__(ExampleClientService.HELLO, self.__bind_helper__(self.hello), ['std::vector<int32_t>'], [])

    def hello(__self__, _req):
        raise NotImplementedError('subclass ExampleClientService and implement your own hello function')

class ExampleClientProxy(object):
    def __init__(self, clnt):
        self.__clnt__ = clnt

    def async_hello(__self__, _req):
        return __self__.__clnt__.async_call(ExampleClientService.HELLO, [_req], ExampleClientService.__input_type_info__['hello'], ExampleClientService.__output_type_info__['hello'])

    def sync_hello(__self__, _req):
        __result__ = __self__.__clnt__.sync_call(ExampleClientService.HELLO, [_req], ExampleClientService.__input_type_info__['hello'], ExampleClientService.__output_type_info__['hello'])
        if __result__[0] != 0:
            raise Exception("RPC returned non-zero error code %d: %s" % (__result__[0], os.strerror(__result__[0])))
        if len(__result__[1]) == 1:
            return __result__[1][0]
        elif len(__result__[1]) > 1:
            return __result__[1]

