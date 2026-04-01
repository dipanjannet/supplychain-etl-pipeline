import requests

response = requests.get("https://api.escuelajs.co/api/v1/products")
products = response.json()
print(products[0])
