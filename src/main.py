import json, sys
from app import create_app
from node import Node

def main():
    if len(sys.argv) != 3:
        sys.exit(1)
    
    path_to_config = sys.argv[1]
    index = int(sys.argv[2])

    with open(path_to_config, 'r') as file:
        config = json.load(file)["addresses"]

    if config:
        url_info = config[index]
        ip = url_info["ip"]
        port = url_info["port"]
        max_num_votes = len(config)

        node = Node(index, config, ip, port, max_num_votes)
        app = create_app(node)
        app.run(host=node.ip, port=node.port, debug=False)
    else:
        sys.exit(1)


if __name__ == '__main__':
    main()