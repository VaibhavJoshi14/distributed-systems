from concurrent import futures
import logging

import grpc
import market_pb2
import market_pb2_grpc
import pprint

SELLERS = {} # contains (key, value) pairs of sellers as (uuid, {"seller_address": ip:port, "notification_address": ip:port)
ITEMS = {} # contains (key, value) pairs of items as (item_id, {item_id, type, name, quantity, description, price, rating, seller_address, seller_uid}).
           # rating is the average of all ratings done by clients.
RATINGS = {} # is a dictionary with key-value pairs as (item_id, {(client_address, rate-int)})
WISH_LIST = {} # contains (key, value) pairs as (client_address, [wished item-ids])
CLIENT_NOTIFICATION_ADDRESSES = {} #contains (key, value) pairs as (client_address, notification_address)
item_id = 0

class Register(market_pb2_grpc.RegisterServicer):
    def RegisterSeller(self, request, context):
        print("\n-------------------------------------------------------------------------")
        print("\nMarket received register request:")
        print(request.__str__())
        if request.uid not in SELLERS:
            SELLERS[request.uid] = {}
            SELLERS[request.uid]["seller_address"] = request.seller_address
            SELLERS[request.uid]["notification_address"] = request.notification_address
            response = "SUCCESS"
        else:
            response = "FAILED: uuid already exists."
        print("Responding with:", response)
        return market_pb2.Reply(message=response)


class Sell(market_pb2_grpc.SellServicer):
    def SellItem(self, request, context):
        print("\n-------------------------------------------------------------------------")
        if request.seller_uid in SELLERS:
            print("\nMarket received a new ItemSell request from seller uid:", request.seller_uid)
        else:
            return market_pb2.NewItemReply(fail = "FAILED: not registered as a seller in the market. Please register.")
        if request.ELECTRONICS == True:
            Type = "ELECTRONICS"
        elif request.FASHION == True:
            Type = "FASHION"
        else:
            Type = "OTHERS"
        
        global item_id
        item_id += 1
        ITEMS[item_id] = {"item_id":item_id, "type": Type, "name": request.product_name, "quantity": request.quantity, "description": request.description,
                        "rating": None, "seller_address": request.seller_address, "price": request.price_per_unit_INR, 
                        "seller_uid": request.seller_uid}
        
        print("New item registered with details:")
        pprint.pprint(ITEMS[item_id])
        return market_pb2.NewItemReply(item_id=item_id)
    


class Update(market_pb2_grpc.UpdateServicer):
    def UpdateItemRequest(self, request, context):
        print("\n-------------------------------------------------------------------------")
        if request.seller_uid not in SELLERS:
            return market_pb2.Reply(message = "FAILED: not registered as a seller in the market. Please register.")
        if request.item_id not in ITEMS:
            return market_pb2.Reply(message = "FAILED: item id not registered.")
        if ITEMS[request.item_id]["seller_address"] != request.seller_address:
            return market_pb2.Reply(message = "FAILED: seller address does not match.")

        ITEMS[request.item_id]["price"] = request.new_price
        ITEMS[request.item_id]["quantity"] = request.new_quantity
        
        print("\nItem update request by (uid, seller_address):", request.seller_uid, request.seller_address, "is successful. One item updated, with details:")
        pprint.pprint(ITEMS[request.item_id])

         # trigger all clients who have wishlisted the product
        print("Notifying all clients who have wishlisted this item")
        for client_uid in WISH_LIST:
            if request.item_id in WISH_LIST[client_uid]:
                #print(WISH_LIST[client_addr])
                with grpc.insecure_channel(CLIENT_NOTIFICATION_ADDRESSES[client_uid]) as channel:
                    stub = market_pb2_grpc.NotifyStub(channel)

                    response = stub.NotifyUpdate(market_pb2.Item2(item_id=request.item_id, type=ITEMS[request.item_id]["type"],
                                                          name=ITEMS[request.item_id]["name"], quantity=ITEMS[request.item_id]["quantity"],
                                                          description=ITEMS[request.item_id]["description"], price=ITEMS[request.item_id]["price"],
                                                          rating=ITEMS[request.item_id]["rating"], seller_address=ITEMS[request.item_id]["seller_address"],
                                                          seller_uid=ITEMS[request.item_id]["seller_uid"]))
       
        return market_pb2.Reply(message = "SUCCESS")


class Delete(market_pb2_grpc.DeleteServicer):
    def DeleteItemRequest(self, request, context):
        print("\n-------------------------------------------------------------------------")
        if request.seller_uid not in SELLERS:
            return market_pb2.Reply(message = "FAILED: not registered as a seller in the market.")
        if request.item_id not in ITEMS:
            return market_pb2.Reply(message = "FAILED: item id not registered.")
        if ITEMS[request.item_id]["seller_address"] != request.seller_address:
            return market_pb2.Reply(message = "FAILED: seller address does not match.")
        print("\nMarket received delete item request:", request.__str__())
        ITEMS.pop(request.item_id)
        print("Item deleted successfully.")
        return market_pb2.Reply(message = "SUCCESS")
     

class Display(market_pb2_grpc.DisplayServicer):
    def DisplaySellerItems(self, request, context):
        print("\n-------------------------------------------------------------------------")
        if request.seller_uid not in SELLERS:
            return market_pb2.ItemList(items=[])
        print("\nMarket received a request to display all seller items: request detail: ", request.__str__())
        item_list = []
        for i_id in ITEMS:
            if ITEMS[i_id]["seller_uid"] == request.seller_uid:
                item_list.append(ITEMS[i_id])
        return market_pb2.ItemList(items=item_list)


class Search(market_pb2_grpc.SearchServicer):
    def SearchItem(self, request, context):
        print("\n-------------------------------------------------------------------------")
        item_list = []
        print("\nMarket received request to search: full request", request.__str__())
        Type = ""
        if request.item_name == "":
            if request.ELECTRONICS == True:
                Type = "ELECTRONICS"
            elif request.FASHION == True:
                Type = "FASHION"
            elif request.OTHERS == True:
                Type = "OTHERS"
            elif request.ANY == True:
                Type = "ALL"
        
    
        for i_id in ITEMS:
            if ITEMS[i_id]["name"] == request.item_name:
                return market_pb2.ItemList(items=ITEMS[i_id])
        
            if Type == "ALL":
                item_list.append(ITEMS[i_id])
                
            elif ITEMS[i_id]["type"] == Type:
                item_list.append(ITEMS[i_id])

        return market_pb2.ItemList(items=item_list)
        
#Also triggers notification to seller of item
class BuyItem(market_pb2_grpc.BuyItemServicer):
    def BuyItemRequest(self, request, context):
        print("\n-------------------------------------------------------------------------")
        print("\nMarket received request to buy:")
        print(request.__str__())
        
        if request.item_id not in ITEMS:
            print("FAIL: item does not exist.")
            return market_pb2.Reply(message="FAIL: item does not exist.")
        if ITEMS[request.item_id]["quantity"] < request.quantity:
            print("FAIL: quantity larger than available.")
            return market_pb2.Reply(message="FAIL: quantity larger than available.")
        
        ITEMS[request.item_id]["quantity"] -= request.quantity
        print("SUCCESS.")
        
        # trigger notification to the seller.
        print("Notifying seller about the purchase")
        with grpc.insecure_channel(ITEMS[request.item_id]["seller_address"]) as channel:
            stub = market_pb2_grpc.NotifyStub(channel)

            response = stub.NotifyUpdate(market_pb2.Item2(item_id=request.item_id, type=ITEMS[request.item_id]["type"],
                                                          name=ITEMS[request.item_id]["name"], quantity=ITEMS[request.item_id]["quantity"],
                                                          description=ITEMS[request.item_id]["description"], price=ITEMS[request.item_id]["price"],
                                                          rating=ITEMS[request.item_id]["rating"], seller_address=ITEMS[request.item_id]["seller_address"],
                                                          seller_uid=ITEMS[request.item_id]["seller_uid"]))

        return market_pb2.Reply(message="SUCCESS.")


class AddToWishList(market_pb2_grpc.AddToWishListServicer):
    def AddToWishListRequest(self, request, context):
        print("\n-------------------------------------------------------------------------")
        print("Market received Add-to-wish-list request:")
        print(request.__str__())

        if request.item_id not in ITEMS:
            print("FAIL: item does not exist.")
            return market_pb2.Reply(message="FAIL: item does not exist.")

        if request.buyer_uid not in WISH_LIST:
            WISH_LIST[request.buyer_uid] = [request.item_id]
        else:
            WISH_LIST[request.buyer_uid].append(request.item_id)
        CLIENT_NOTIFICATION_ADDRESSES[request.buyer_uid] = request.notification_address
        print("SUCCESS.")
        return market_pb2.Reply(message="SUCCESS.")
        

class RateItem(market_pb2_grpc.RateItemServicer):
    def RateItemRequest(self, request, context):
        print("\n-------------------------------------------------------------------------")
        print("Market received Rate request:")
        print(request.__str__())

        if request.item_id not in ITEMS:
            print("FAIL: item does not exist.")
            return market_pb2.Reply(message="FAIL: item does not exist.")

        if request.item_id in RATINGS and request.client_address in RATINGS[request.item_id]:
            print("FAIL: can only rate an item once.")
            return market_pb2.Reply(message="FAIL: can only rate an item once.")
        
        if request.rating > 5 or request.rating < 1:
            print("FAIL: can only rate in an integer from 1 to 5.")
            return market_pb2.Reply(message="FAIL: can only rate in an integer from 1 to 5.")
        
        if request.item_id not in RATINGS:
            RATINGS[request.item_id] = {}
        RATINGS[request.item_id][request.client_address] = request.rating
        if ITEMS[request.item_id]["rating"] == None: 
            ITEMS[request.item_id]["rating"] = request.rating
        else:
            ITEMS[request.item_id]["rating"] = (ITEMS[request.item_id]["rating"] + request.rating) / 2
        print("SUCCESS.")
        
        return market_pb2.Reply(message="SUCCESS.")



def serve():
    port = "50051"
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=1)) #max_workers = 1 necessary for now since mutual exclusion not handled explicitly
    
    market_pb2_grpc.add_RegisterServicer_to_server(Register(), server)
    market_pb2_grpc.add_SellServicer_to_server(Sell(), server)
    market_pb2_grpc.add_UpdateServicer_to_server(Update(), server)
    market_pb2_grpc.add_DeleteServicer_to_server(Delete(), server)
    market_pb2_grpc.add_DisplayServicer_to_server(Display(), server)
    market_pb2_grpc.add_SearchServicer_to_server(Search(), server)
    market_pb2_grpc.add_BuyItemServicer_to_server(BuyItem(), server)
    market_pb2_grpc.add_AddToWishListServicer_to_server(AddToWishList(), server)
    market_pb2_grpc.add_RateItemServicer_to_server(RateItem(), server)
    

    server.add_insecure_port("[::]:" + port)
    server.start()
    print("Server started, listening on " + port)
    server.wait_for_termination()


if __name__ == "__main__":
    logging.basicConfig()
    serve()
