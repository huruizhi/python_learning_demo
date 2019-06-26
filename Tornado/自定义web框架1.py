from wsgiref.simple_server import make_server
import jinja2
# python3执行有问题


def index():
    with open('templates/index1.html') as f:
        file = f.read()
    tpl = jinja2.Template(file)
    file = tpl.render(name='Ydd', name_list=['aa', 'bb', 'cc', 'dd'])
    return file


def login():
    return 'login'


def routers():
    urlpatterns = (
        ('/index/', index),
        ('/login/', login),
    )
    return urlpatterns


def RunServer(environ, start_response):
    start_response('200 OK', [('Content-Type', 'text/html')])
    url = environ['PATH_INFO']
    urlpatterns = routers()
    func = None
    for item in urlpatterns:
        if item[0] == url:
            func = item[1]
            break
    if func:
        string = func()
        return [string.encode('utf-8')]
    else:
        return [b'404 not found']


if __name__ == '__main__':
    httpd = make_server('', 8000, RunServer)
    print("Serving HTTP on port 8000...")
    httpd.serve_forever()
