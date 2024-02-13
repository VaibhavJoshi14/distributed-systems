import logging
from concurrent import futures

import grpc
import market_pb2
import market_pb2_grpc

import uuid
from requests import get


#unique id of seller
uid = str(uuid.uuid1())
#address of market (ip:port)

market_ip = "34.70.175.244"#"localhost" 
market_address = market_ip + ":" + "50051"

notification_port = "50055"
seller_ip = get('https://api.ipify.org').content.decode('utf8') #"localhost"#
seller_address = seller_ip + ":" + notification_port

class Notify(market_pb2_grpc.NotifyServicer):
    def NotifyUpdate(self, request, context):
        print("\n-------------------------------------------------------------------------")
        print("Seller received a notification from market.")
        print(request.__str__())
        return market_pb2.Reply(message="SUCCESS.")

def run():
    notif_server = grpc.server(futures.ThreadPoolExecutor(max_workers=1)) #max_workers = 1 necessary for now since mutual exclusion not handled explicitly
    market_pb2_grpc.add_NotifyServicer_to_server(Notify(), notif_server)
    notif_server.add_insecure_port("[::]:" + notification_port)
    notif_server.start()
    print("Notification server started, listening on " + notification_port)

    with grpc.insecure_channel(market_address) as channel:
        # a seller registers for the first time (calls RegisterSeller).
        print("\n-------------------------------------------------------------------------")
        print("\nSending RegisterSeller request from (uuid, ip:port)=(", uid, ", ", seller_address, ")", sep="")
        stub = market_pb2_grpc.RegisterStub(channel)
        response = stub.RegisterSeller(market_pb2.SellerRegisterRequest(uid=uid, seller_address=seller_address, notification_address=seller_address))
        print("Seller received: " + response.message)
    
    
        # Seller calls SellItem() to post a new item on the market : Iphone
        print("\n-------------------------------------------------------------------------")
        print("\nSending SellItem request from (uuid, ip:port)=(", uid, ", ", seller_address, ")", sep="")
        stub_sell = market_pb2_grpc.SellStub(channel)
        response = stub_sell.SellItem(market_pb2.NewItem(seller_uid=uid, ELECTRONICS=True, product_name="IPhone 15", description="Apple Iphone in color black", price_per_unit_INR=100000, quantity=100, seller_address=seller_address))
        print("Seller received: ", response.__str__())


        # Seller registers for another item : mac
        print("\n-------------------------------------------------------------------------")
        print("\nSending SellItem request from (uuid, ip:port)=(", uid, ", ", seller_address, ")", sep="")
        #stub = market_pb2_grpc.SellStub(channel)
        response = stub_sell.SellItem(market_pb2.NewItem(seller_uid=uid, ELECTRONICS=True, product_name="Mac book pro", description="Apple mac book pro", price_per_unit_INR=109000, quantity=200,seller_address=seller_address))
        print("Seller received: ", response.__str__())

    
        # seller asks for displaying all its items
        print("\n-------------------------------------------------------------------------")
        print("\nSending request to display all items")
        stub = market_pb2_grpc.DisplayStub(channel)
        response = stub.DisplaySellerItems(market_pb2.DisplayAllRequest(seller_uid=uid, seller_address=seller_address))
        print("Seller received: ", response.__str__())

        import time 
        time.sleep(10) # sleeps for 10 seconds

        # Seller updates the details of item 'chair'.
        print("\n-------------------------------------------------------------------------")
        print("\nSending UpdateItem request from (uuid, ip:port)=(", uid, ", ", seller_address, ")", sep="")
        stub = market_pb2_grpc.UpdateStub(channel)
        response = stub.UpdateItemRequest(market_pb2.UpdateItem(seller_uid=uid, item_id=4, new_price=90000, new_quantity=300, seller_address=seller_address))
        print("Seller received: ", response.__str__())


    notif_server.wait_for_termination() 

if __name__ == "__main__":
    logging.basicConfig()
    run()
