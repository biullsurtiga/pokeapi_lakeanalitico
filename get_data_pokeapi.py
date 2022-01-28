import requests
import json

def get_data(endpoint,id):
    url = f"http://pokeapi.co/api/v2/{endpoint}/{id}"
    response = requests.get(url)
    if response.status_code != 200:
        if response.status_code == 404:
            print("Resorce not found for {0}".format(id))
            return None
        else:
            raise Exception("API Hit Failed - {0}".response.text)
    return response

def write_to_file(endpoint, min_ids, max_ids, name_file):
    output_file = f"./data/files/{name_file}.json"
    for id in range(min_ids, max_ids):
        response = get_data(endpoint, id)
        if response:
            with open(output_file, 'a') as f:
                f.write(json.dumps(response.json()))
                f.write("\n")
    return output_file


if __name__ == "__main__":

# POKEMON
    min_ids = 0
    max_ids = 900
    json_file = write_to_file("pokemon", min_ids, max_ids, "pokemons")
    print("POKEMONS CARREGADOS")

# MEGA POKEMON
    min_ids = 10000
    max_ids = 10222
    json_file = write_to_file("pokemon", min_ids, max_ids, "pokemons")
    print("MEGA POKEMONS CARREGADOS")

# EVOLUTION CHAINS
    min_ids = 0
    max_ids = 480
    json_file = write_to_file("evolution-chain", min_ids, max_ids, "evolutions-chain")
    print("EVOLUTION CHAINS CARREGADOS")

# EVOLUTION TRIGGERS
    min_ids = 0
    max_ids = 15
    json_file = write_to_file("evolution-trigger", min_ids, max_ids, "evolutions-trigger")
    print("EVOLUTION TRIGGERS CARREGADOS")

# TYPES
    min_ids = 0
    max_ids = 20
    json_file = write_to_file("type", min_ids, max_ids, "types")
    print("TYPES")
