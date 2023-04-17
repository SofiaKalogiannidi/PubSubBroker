import sys
import socket
import threading
from _thread import*

subscribers_dict={}
publishers_dict={}


def count_messages(mydict):
    len=0
    for topic in mydict.keys():
        for x in mydict.get(topic):
            len+=1
    return len

def send_topics(id,already_sent):
    user_dict={}
    user_dictfinal={}
    for topic in publishers_dict.keys():
        if subscribers_dict.get(topic)!=None and id in subscribers_dict.get(topic) :
            user_dict[topic]=publishers_dict.get(topic)
    #print("USER DICT BEFORE FILTERING",user_dict)
    for topic in user_dict.keys():
        user_dictfinal[topic]=[]
        for message in user_dict[topic]:
            #print(message)
            if message not in already_sent:
                #print("MESSAGE NOT IN ALREADY SENT",message)
                user_dictfinal[topic].append(message)
    #print("ALREADY SENT",already_sent)
    #print("USER DICT AFTER FILTERING",user_dictfinal)
    return user_dictfinal
            

def subscriber_request(message):
 
    words = [word for word in message.split()]
    if (words[1]=='sub'):
        if(subscribers_dict.get(words[2])==None):
                L=[words[0]]
                subscribers_dict[words[2]]=L
        else:
            subscribers_dict[words[2]].append(words[0])
        
    elif (words[1]=='unsub'):
        if (words[0] in subscribers_dict.get(words[2]) ):
            subscribers_dict.get(words[2]).remove(words[0])
    return words[0]
            
def publisher_request(message):
    words = [word for word in message.split()]
    if (words[1]=='pub'):
       Topic=words[2]
       Message=" ".join(words[3:])
       Message.replace("\n", "")
       if(publishers_dict.get(words[2])==None):
            L=[Message]
            publishers_dict[Topic]=L
       else:
            publishers_dict[Topic].append(Message)
            
#me pollous subscribers
def on_new_subscriber(sub):
      already_sent={}
      while True:
        msg = sub.recv(1024).decode()
        print("Message received from subscriber:")
        print(msg)
        id=subscriber_request(msg)
        if already_sent.get(id)==None:
            already_sent[id]=[]

        if not msg:
           
            break
        print("Sending acknowledgment to the subscriber.")
        
        msg_out = " OK ".encode()
        sub.send(msg_out)
        
        my_messages=send_topics(id,already_sent[id])
        messages=count_messages(my_messages)
        print('I need to send messages',messages)
       
        msg_out = str(messages).encode('utf8')
        sub.send(msg_out)
        
        for topic in my_messages:
            for x in my_messages.get(topic):
              
                already_sent[id].append(x)
                msg_out="Received msg for topic"+topic+":"+x+"\n"
                #print(msg_out)
                msg_out=msg_out.encode()
                sub.sendall(msg_out)
                
     
        
      sub.close()
      
      
#me pollous publishers
def on_new_publisher(pub):
      while True:
        
        
        msg = pub.recv(1024).decode()
        print("Message received from publisher:")
        print(msg)
        publisher_request(msg)
        #print(publishers_dict)
        if not msg:
            break
        print("Sending acknowledgment to the publisher.")
        msg_out = " OK".encode()
        pub.send(msg_out)
      pub.close()


        

def main(argv):
    #command line arguments
    for i in range(1,len(sys.argv)):
        if(sys.argv[i]=='-s'):
           SubscriberPort=int(sys.argv[i+1])
        elif(sys.argv[i]=='-p'): 
            PublisherPort=int(sys.argv[i+1])
        
    print("subsciber port is:",SubscriberPort)
    print("publisher port is:",PublisherPort)
  
   
    
    subscribersCount=0
    publishersCount=0

    #me pollous subscribers
    
    mySocketSub = socket.socket(family=socket.AF_INET, type=socket.SOCK_STREAM, proto=0, fileno=None)
    print("Socket created.")
    hostname = 'localhost'
    mySocketSub.bind((hostname, SubscriberPort))
    mySocketSub.listen(5)
    mySocketPub = socket.socket(family=socket.AF_INET, type=socket.SOCK_STREAM, proto=0, fileno=None)
    hostname = 'localhost'
    mySocketPub.bind((hostname, PublisherPort))
    mySocketPub.listen(5)
    print("Waiting for Publishers/Subscribers.")
  
    while subscribersCount<=5 and publishersCount<=5:
       c, addr = mySocketSub.accept() 
       print("Connection established with subscriber")
       subscribersCount+=1
       start_new_thread(on_new_subscriber,(c,))
       c, addr = mySocketPub.accept() 
       print("Connection established with publisher")
       publishersCount+=1
       start_new_thread(on_new_publisher,(c,))

    mySocketSub.close()
    mySocketPub.close()
    
    

if __name__ == "__main__":
   main(sys.argv[1:])


