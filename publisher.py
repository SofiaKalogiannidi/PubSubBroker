import sys
import os
import time
import socket


def print_message(msg):
    
    words = [word for word in msg.split()]
    if (words[1]=='pub'):
       Topic=words[2]
       Message=" ".join(words[3:])
       print("Published message for topic "+Topic+" : "+Message)
       
def process_file(filename,id):
    L=[]
    if os.path.isfile(filename):
        with open(filename) as file_in:
            for line in file_in:
                words = [word for word in line.split()]
                time_wait=int(words[0])
                words[0]=id
                msg = " ".join(words)
                X=(time_wait,msg)
                L.append(X)
    else:
        print("This file does not exist")
    return L

def get_user_input(id):
    print("Give a publish command")
    command=input()
    words = [word for word in command.split()]
    time_wait=int(words[0])
    words[0]=id
    msg = " ".join(words)
    return time_wait,msg
  

def main(argv):
    filexists=False
    for i in range(1,len(sys.argv)):
        if(sys.argv[i]=='-i'):
           id=sys.argv[i+1]
        elif(sys.argv[i]=='-r'): 
            port=sys.argv[i+1]
        elif(sys.argv[i]=='-h'):
            BrokerIP=sys.argv[i+1]
        elif(sys.argv[i]=='-p'):
           BrokerPort=int(sys.argv[i+1])
        elif(sys.argv[i]=='-f'):
            filexists=True
            filename=sys.argv[i+1]
    print("id is:",id)
    print("port is:",port)
    print("Ip pf Broker is:",BrokerIP)
    print("Port of Broker is:",BrokerPort)
    print("filename is:",filename)
    
    
    mySocket = socket.socket(family=socket.AF_INET, type=socket.SOCK_STREAM, proto=0, fileno=None)
    print("Socket created.")
    hostname = socket.gethostbyaddr(BrokerIP)
    mySocket.connect((hostname[0], BrokerPort))
    print("Connection established with the server.")
    isExecuted=False
    while True:
        if(filexists==True and isExecuted==False):
            L=process_file(filename,id)
            for command in L:
               print("Sleeping for",command[0])
               time.sleep(command[0]) 
               print("Sending msg to the server:", command[1])
               mySocket.send(command[1].encode())
               msg_in = mySocket.recv(1024).decode()
               print("Acknowledgment received from the server:")
               print(msg_in)
               print_message(command[1])
        isExecuted=True
        time_wait,msg = get_user_input(id)
        print("Sleeping for",time_wait)
        time.sleep(time_wait)
        print("Sending msg to the server:", msg)
        mySocket.send(msg.encode())
        msg_in = mySocket.recv(1024).decode()
        print("Acknowledgment received from the server:")
        print(msg_in)
        print_message(msg)
    

if __name__ == "__main__":
   main(sys.argv[1:])