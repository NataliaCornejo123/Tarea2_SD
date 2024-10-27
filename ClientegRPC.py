import grpc
import orders_pb2
import orders_pb2_grpc

def get_order_data():
    print("Ingrese los detalles del pedido:")
    product_name = input("Nombre del producto: ")
    price = float(input("Precio: "))
    payment_gateway = input("Medio de pago: ")
    card_brand = input("Tipo de tarjeta: ")
    bank = input("Banco: ")
    region = input("Región de envío: ")
    address = input("Dirección de envío: ")
    email = input("Correo electrónico: ")
    return orders_pb2.Order(
        product_name=product_name,
        price=price,
        payment_gateway=payment_gateway,
        card_brand=card_brand,
        bank=bank,
        region=region,
        address=address,
        email=email,
    )

def run():
    with grpc.insecure_channel('localhost:50051') as channel:
        stub = orders_pb2_grpc.OrderManagementStub(channel)

        order = get_order_data()
        response = stub.CreateOrder(order)

        print(f"Respuesta del servidor: {response.status}, ID del pedido: {response.order_id}")

if __name__ == '__main__':
    run()

