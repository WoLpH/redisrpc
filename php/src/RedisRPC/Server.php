<?php

# Copyright (C) 2012.  Nathan Farrington <nfarring@gmail.com>
# 
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
# 
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
# 
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.
namespace RedisRPC;

use Predis;

# Ref: http://www.php.net/manual/en/language.constants.php
if (!function_exists("debug_print")) { 
    if ( defined('DEBUG') && TRUE===DEBUG ) { 
        function debug_print($string,$flag=NULL) { 
            /* if second argument is absent or TRUE, print */ 
            if ( !(FALSE===$flag) ) 
                #print 'DEBUG: '.$string . "\n"; 
                print $string . "\n"; 
        } 
    } else { 
        function debug_print($string,$flag=NULL) { 
        } 
    } 
} 

/**
 * Executes function calls received from a Redis queue.
 *
 * @author Nathan Farrington <nfarring@gmail.com>
 */
class Server {

    private $pubsub;
    private $redis_args;
<<<<<<< HEAD
    private $redis_server;
    private $redis_pubsub_server;
    private $message_queue;
    private $local_object;
=======
    private $local_objects;
>>>>>>> 5dbe1c85f67f4cff949a2ef4a5b5f46f66c4a85a

    /**
     * Initializes a new server.
     *
     * @param mixed $redis_args Redis server arguments.
     * @param mixed $local_object Handle to local wrapped objects as
     *        associative array (key = queue name) that will receive the RPC calls.
     */
<<<<<<< HEAD
    public function __construct($redis_args, $message_queue, $local_object) {
        $this->pubsub = null;
        $this->redis_args = $redis_args;
        $this->redis_pubsub_server = null;
        $this->message_queue = $message_queue;
        $this->local_object = $local_object;
=======
    public function __construct($redis_args, $local_objects) {
        $this->redis_args = $redis_args;
        $this->local_objects = $local_objects;
>>>>>>> 5dbe1c85f67f4cff949a2ef4a5b5f46f66c4a85a
    }

    public function __destruct(){
        unset($this->pubsub);
        if($this->redis_pubsub_server){
            $this->redis_pubsub_server->disconnect();
        }
    }

    /**
     * Starts the server.
     */
    public function run() {
        $this->redis_pubsub_server = new Predis\Client($this->redis_args);
        $subscribers = $this->redis_pubsub_server->pubsub('numsub', $this->message_queue);
        if($subscribers[$this->message_queue] != 0){
            throw new \RuntimeException('Server already running for queue ' .
                $this->message_queue);
        }

        $this->pubsub = $this->redis_pubsub_server->pubSubLoop();

        foreach($this->local_objects as $key => $local_object){
            $pubsub->subscribe($key);
        }

        foreach($pubsub as $message){
            # Pop a message from the queue.
            # Decode the message.
            # Check that the function exists.
            if($message->kind != 'message'){
                debug_print('Ignoring ' . $message->kind . ': ' . $message->payload);
                continue;
            }

            // assert($message->channel == $this->message_queue);
            // $message_queue = $message

            debug_print('RPC Request: ' . $message->payload);
            $rpc_request = json_decode($message->payload);
            $response_queue = $rpc_request->response_queue;
            $function_call = FunctionCall::from_object($rpc_request->function_call);
            if (!method_exists($this->local_object, $function_call->name)) {
                $rpc_response = array(
                    'exception' => 'method "' . $function_call->name . 
                    '" does not exist');
            }else{
                $code = $function_call->name;
                $code .= json_encode($function_call->args);
                debug_print($code);

                try {
                    $return_value = call_user_func_array(
                        array($this->local_object, $function_call->name),
                        $function_call->args);

                    $rpc_response = array(
                        'return_value' => $return_value,
                        'return_type' => gettype($return_value) == 'object'
                        ? get_class($return_value) : null,
                    );
                    print_r($rpc_response);
                }catch (\Exception $e) {
                    $rpc_response = array(
                        'exception' => $e->getMessage(),
                        'exception_type' => gettype($e) == 'object'
                        ? get_class($e) : null,
                    );
                    if(method_exists($e, 'getResponse')){
                        $rpc_response['response'] = $e->getResponse();
                        $rpc_response['response_type'] =
                            gettype($e->getResponse())
                            ? get_class($e->getResponse()) : null;
                    }
                }
            }

            $message = json_encode($rpc_response);

            debug_print("RPC Response: $message");
            assert($redis_server->pubsub('numsub', $this->response_queue) > 0);
            $redis_server = new Predis\Client($this->redis_args);
            $redis_server->publish($response_queue, $message);
            unset($redis_server);
        }
    }
}

?>
