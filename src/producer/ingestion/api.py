import requests

response = requests.get("https://fakestoreapi.com/products")
products = response.json()
print(products[0])
