def file1 = 'file1'
def file2 = 'file2'

def example = '127.0.0.1 - - [24/Apr/2011:04:06:01 -0400] "GET /~strabal/grease/photo9/927-3.jpg HTTP/1.1" 200 40028 "-" "Mozilla/5.0 (compatible; YandexImages/3.0; +http://yandex.com/bots)"'

def log = "${getIP()} - - [${getDate()}] \"GET ${getPath()} HTTP/1.1\" 200 40028 \"-\" \"${getBrowser()} (compatible; YandexImages/3.0; +http://yandex.com/bots)\""

def getIP() {

}

def getDate(){

}

def getPath() {

}

def getBrowser() {

}