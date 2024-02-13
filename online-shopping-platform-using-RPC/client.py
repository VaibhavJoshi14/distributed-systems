# Buyer
import logging
from concurrent import futures

import grpc
import market_pb2
import market_pb2_grpc
import uuid
from requests import get

market_ip = "34.70.175.244"#"localhost" 
market_address = market_ip + ":" + "50051"

notification_port = "50053"
buyer_ip = get('https://api.ipify.org').content.decode('utf8') #"localhost"#
buyer_address = buyer_ip 
notification_address = buyer_ip + ":" + notification_port
uid = str(uuid.uuid1())

class Notify(market_pb2_grpc.NotifyServicer):
    def NotifyUpdate(self, request, context):
        print("\n-------------------------------------------------------------------------")
        print("Client received a notification from market.")
        print(request.__str__())
        return market_pb2.Reply(message="SUCCESS.")

def run():
    notif_server = grpc.server(futures.ThreadPoolExecutor(max_workers=1)) #max_workers = 1 necessary for now since mutual exclusion not handled explicitly
    market_pb2_grpc.add_NotifyServicer_to_server(Notify(), notif_server)
    notif_server.add_insecure_port("[::]:" + notification_port)
    notif_server.start()
    print("Notification server started, listening on " + notification_port)

    with grpc.insecure_channel(market_address) as channel:
        # a client sending a search request type ANY
        print("\n-------------------------------------------------------------------------")
        print("\nClient sending SearchItem request type ANY from their address: " , buyer_address, sep="")
        stub = market_pb2_grpc.SearchStub(channel)
        response = stub.SearchItem(market_pb2.SearchRequest(ANY=True, client_address=buyer_address))
        print("Client received:\n" + response.__str__())

        # client sending request to buy item_id = 3
        print("\n-------------------------------------------------------------------------")
        print("\nClient sending BuyItem request of item_id=3 from their address: " , buyer_address, sep="")
        stub = market_pb2_grpc.BuyItemStub(channel)
        response = stub.BuyItemRequest(market_pb2.Buy(item_id=3, quantity=1, buyer_address=buyer_address))
        print("Client received:\n" + response.__str__())

        # client sending add to wish list request of item_id = 2
        print("\n-------------------------------------------------------------------------")
        print("\nClient sending AddToWishList request of item_id=2 from their address: " , buyer_address, sep="")
        stub = market_pb2_grpc.AddToWishListStub(channel)
        response = stub.AddToWishListRequest(market_pb2.Wish(item_id=2, buyer_address=buyer_address, notification_address=notification_address, buyer_uid=uid))
        print("Client received:\n" + response.__str__())
        
        #client sending a rate request of item_id = 3
        print("\n-------------------------------------------------------------------------")
        print("\nClient sending rate=4 request of item_id=3 from their address: " , buyer_address, sep="")
        stub = market_pb2_grpc.RateItemStub(channel)
        response = stub.RateItemRequest(market_pb2.Rate(item_id=3, client_address=buyer_address, rating=4))
        print("Client received:\n" + response.__str__())

    notif_server.wait_for_termination()

if __name__ == "__main__":
    logging.basicConfig()
    run()