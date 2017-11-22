from twython import TwythonStreamer
import socket
import re
import codecs

def get_connection():
    s = socket.socket()
    s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)

    host = "localhost"      # Get local machine name
    port = 5555                 # Reserve a port for your service.
    s.bind((host, port))        # Bind to the port

    print("Listening on port: %s" % str(port))

    s.listen(5)                 # Now wait for client connection.
    c, addr = s.accept()        # Establish connection with client.

    print( "Received request from: " + str( addr ) )
    return c

def format_date(string_in):

    def tell_month(month):
        months = {u"Jan":u"01", u"Feb":u"02", u"Mar":u"03", u"Apr":u"04", u"May":u"05", u"Jun":u"06", u"Jul":u"07", u"Aug":u"08", u"Sep":u"09", u"Oct":u"10", u"Nov":u"11", u"Dec":u"12" }
        return months[month]
    if len(string_in) > 5:
        time_list = string_in.split()
        month_number = tell_month(time_list[1])
        formatted_time = time_list[5] + "-" + month_number + "-" + time_list[2] + " " + time_list[3]
        return formatted_time
    else:
        return "null"

def get_lang(lang_pre):
    if lang_pre == "en":
       return  lang_pre
    elif lang_pre == "und":
       return u"xx"
    elif lang_pre == "de":
       return lang_pre
    else:
       return "else"

def get_city(place):
    if type(place) is dict:
        city = place["name"]
        return city
    else:
        try:
            city_list = re.findall("u'full_name': u'(\w+)'", place)
            city_name =  city_list[0].decode("utf-8")
            return city_name
        except:
            return u"Unknown"

def get_country(place):
    if type(place) is dict:
        country = place["country_code"]
        return country
    else:
        try:
            country_list = re.findall("u'country_code': u'(\w+)'", place)
            counrty_name =  country_list[0].decode("utf-8")
            return country_name
        except:
            return u"XX"

def get_user_name(user):
    try:
        user_list = re.findall("u'screen_name': u'(\w+)'", user)
        user_name =  user_list[0].decode("utf-8")
        return user_name
    except:
        return u"Unknown"

#f = codecs.open('thesisFiles/twitterJSON', mode='a', encoding='utf-8')

class MyStreamer(TwythonStreamer):

    def __init__(self, app_key, app_secret, oauth_token, oauth_token_secret, c_socket):
        TwythonStreamer.__init__(self, app_key, app_secret, oauth_token, oauth_token_secret)
        self.c_socket = c_socket

    def on_success(self, data):
        text = data.get('text', u'NULL').replace('\n', ' ').replace('"', ' ').replace('\'', '')
        date_time_raw = data.get('created_at', u'NULL')
        date_time = format_date(date_time_raw)
#        entities = unicode(data.get('entities', 'NULL'))
        tweet_id = data.get('id_str', u'NULL')
        lang_pre = data.get('lang', u'xx')
        lang = get_lang(lang_pre)
        place = data.get('place', u'NULL')
        city = get_city(place)
        country = get_country(place)
        user_dict = str(data.get('user', u'NULL'))
        user = get_user_name(user_dict)
        tweet_json = u'{ "text":"'+text+u'", "tweet_id":"'+tweet_id+u'", "date_time":"'+date_time+u'", "lang":"'+lang+u'", "city":"'+city+u'", "country":"'+country+u'", "user":"'+user+u'" }\n'
#        print(text,date_time,tweet_id,lang,user,city,country)

        if date_time != "null" and lang != "else": #and country == "DE":
#            f.write(tweet_json)
#            print(tweet_json)
            self.c_socket.send(tweet_json.encode('utf-8'))

    def on_error(self, status_code, data):
        print status_code

        # Want to stop trying to get data because of the error?
        # Uncomment the next line!
        #  self.disconnect()

def sendData(c):
    app_key='LoEkASen1j7eZaRILaMg0QeZL'
    app_secret='vBtkLYOblQrroJZKY28epGrwi4JgM8u5YIPzngijR9qVQfzUm8'

    oauth_token='856470008418926592-vblfxUjyPIWNBsWyTs3JCMIOR5iJOpR'
    oauth_secret='MEaSdSi0okLssNYcLCzXGsFiFzPpQysF1lBnttqGfZluh'

    stream = MyStreamer(app_key, app_secret, oauth_token, oauth_secret, c)
    stream.statuses.filter(locations="-25.9,35.5,53.7,69.7")

if __name__ == "__main__":
    c = get_connection()
    sendData(c)
