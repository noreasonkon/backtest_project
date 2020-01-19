
class CrawlerLog(object):

    def __call__(self, f):
        def func(*args, **kwargs):
            mod_name = args[0].__class__.__name__
            print('--- %s ---' % mod_name)
            try:
                result = f(*args, **kwargs)
                print('# success')
                return result
            except Exception as e:
                print('# fail: %s' % e)
        return func
