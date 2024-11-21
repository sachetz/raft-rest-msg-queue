from flask import jsonify, request, current_app
from utils.states import STATES

def init_routes(app):
    @app.route('/request_vote', methods=['PUT'])
    def request_vote():
        node = current_app.config['NODE']
        data = request.get_json()
        term_validation = node.term_check(data.get('term'))
        if not term_validation:
            print(f"Node {node.id}: Term validation failed for request")
            voteGranted = False
        else:
            print(f"Node {node.id}: Term validation succeeded for request")
            can_vote = node.vote_if_can(data)
            voteGranted = can_vote
            if can_vote:
                print(f"Node {node.id}: Can vote, resetting election timer")
                # A server remains in follower state as long as it receives valid RPCs from a leader or candidate
                with node.lock:
                    node.election_timer.reset()
        with node.lock:
            response = {
                'voteGranted': voteGranted,
                'term': node.term
            }
        print(f"Node {node.id}: Sending response {response}")
        return jsonify(response), 200

    @app.route('/append_entries', methods=['PUT'])
    def append_entries():
        node = current_app.config['NODE']
        data = request.get_json()
        response = {}
        # Another server establishes itself as leader
        # If the term in the RPC is smaller than the candidate’s current term, then the candidate rejects the RPC and
        # continues in candidate state
        term_validation = node.term_check(data.get('term'))
        if not term_validation:
            print(f"Node {node.id}: Term validation failed for request")
            response['success'] = False
        else:
            node.apply_pending_logs()
            print(f"Node {node.id}: Term validation succeeded for request")
            response['success'] = node.check_prev_log_match(data)
            node.check_if_candidate()
            if data and 'command' in data:
                node.commit_to_log(data, data('command'))
            # A server remains in follower state as long as it receives valid RPCs from a leader or candidate
            print(f"Node {node.id}: Resetting election timer")
            with node.lock:
                node.election_timer.reset()
        with node.lock:
            response['term'] = node.term
        print(f"Node {node.id}: Sending response {response}")
        return jsonify(response), 200

    @app.route('/status', methods=['GET'])
    def get_status():
        node = current_app.config['NODE']
        with node.lock:                                             # Obtain the lock
            role = node.state                                       # Send the current state
            term = node.term                                        # Send the current term
        return jsonify({"role": STATES[role], "term": term}), 200

    @app.route('/topic', methods=['PUT'])
    def create_topic():
        node = current_app.config['NODE']
        if not node.is_leader():
            return jsonify({"success": False, "term": node.term}), 200
        data = request.get_json()
        topic = data.get('topic')
        if not topic:
            return jsonify({'success': False}), 400
        with node.lock:
            if topic in node.message_topics:
                return jsonify({'success': False}), 409
        prev_log_index, entries = node.commit_to_log(data, "ADD_TOPIC")
        node.heartbeat(called_by_thread=False, prevLogIndex=prev_log_index, entries=entries)
        res = node.check_and_apply_log()
        return jsonify({'success': True}), 201

    @app.route('/topic', methods=['GET'])
    def get_topics():
        node = current_app.config['NODE']
        if not node.is_leader():
            return jsonify({"success": False, "term": node.term}), 200
        node.commit_to_log(None, "GET_TOPICS")
        node.heartbeat()
        res = node.check_and_apply_log()
        return jsonify({'success': True, 'topics': res}), 200

    @app.route('/message', methods=['PUT'])
    def add_message():
        node = current_app.config['NODE']
        if not node.is_leader():
            return jsonify({"success": False, "term": node.term}), 200
        data = request.get_json()
        topic = data.get('topic')
        message = data.get('message')
        if not topic or not message:
            return jsonify({'success': False}), 400
        elif topic not in node.message_topics:
            return jsonify({'success': False}), 404
        else:
            node.commit_to_log(data, "PUT_MESSAGE")
            node.heartbeat()
            res = node.check_and_apply_log()
            return jsonify({'success': True}), 201

    @app.route('/message/<topic>', methods=['GET'])
    def get_message(topic):
        node = current_app.config['NODE']
        if not node.is_leader():
            return jsonify({"success": False, "term": node.term}), 200
        if topic not in node.message_topics or len(node.message_topics[topic]) == 0:
            return jsonify({'success': False}), 404
        node.commit_to_log({'topic': topic}, "GET_MESSAGE")
        node.heartbeat()
        res = node.check_and_apply_log()
        return jsonify({'success': True, 'message': res}), 200
