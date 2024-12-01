import requests
import pprint

headers = {'Authorization': 'Bearer FpcpVRXdvIlV0UAeTmJDWLcc1KprazYLbcLrWhSFgZ5PfLAeixyzThvooiyBjjgiao05luvTUep2viXDqouigM2ZpXmD0OYeo5RkxKDAWVX1NteFHeJWBYacjHa1Zjlv'}
url = f"https://integration.checklistfacil.com.br/v2/evaluations/90152742"

requisicao = requests.get(url, headers=headers, verify=False)
informacoes = requisicao.json()

#primeiro_item = informacoes['categories']
pprint.pprint(informacoes)