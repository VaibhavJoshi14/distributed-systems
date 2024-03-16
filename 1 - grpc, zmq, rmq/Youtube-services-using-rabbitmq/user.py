import pika 
import sys

#an option for login should be added 

class User:
    def __init__(self,username):
        self.username=username
        
        #establishing connection
        self.connection=pika.BlockingConnection(pika.ConnectionParameters('35.225.139.226'))
        self.channel=self.connection.channel()
        self.channel.queue_declare(queue='user_queue')
        
    # def user_login(self):
    #     message=f"{self.username}:{'NULL'}:{'login'}"
    #     self.channel.basic_publish(exchange='', routing_key='user_queue', body=message)
    #     print("SUCCESS: Login request sent to server.")
        
        
    def update_subscription(self, youtuber_name, subscribe_flag):
        message=f"{self.username}:{youtuber_name}:{'subscribe' if (subscribe_flag == True) else 'unsubscribe'}"
        self.channel.basic_publish(exchange='', routing_key='user_queue', body=message)
        if (subscribe_flag == True):
            print("SUCCESS: Subscription request sent to server.")
        else:
            print("SUCCESS: Unsubscribe request sent to server.")
    
    def start_receive_notifications(self):
        def callback(ch, method, properties, body):
            print(f"New Notification: {body}")
        
        print("hello please give me notifications, I'm login")
        self.channel.queue_declare(queue=f'{self.username}_notifications')
        self.channel.basic_consume(queue=f'{self.username}_notifications', on_message_callback=callback, auto_ack=True)
        print("consumed")
        
        self.channel.start_consuming()
        print("after start consuming")
        
        
        
        
        
        
    def subscribe(self,youtuber_name):
        self.update_subscription(youtuber_name,True)
    
    def unsubscribe(self,youtuber_name):
        self.update_subscription(youtuber_name,False)
        
    
        
    


if __name__=="__main__":
    if len(sys.argv) > 4 or  len(sys.argv) ==1:
        print("wrong way of entering data")
        sys.exit(1)

    username=sys.argv[1]
    user=User(username)
    
    
    if len(sys.argv)==2:
        user.start_receive_notifications()
    
    if len(sys.argv) ==4:
        action=sys.argv[2]
        youtuber_name=sys.argv[3]
        
        if action=='s':
            user.subscribe(youtuber_name)
        elif action=='u':
            user.unsubscribe(youtuber_name)
        else:
            print("WARNING: Invalid action")
            sys.exit(1)
    
    user.connection.close()