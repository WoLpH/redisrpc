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

    private $redis_args;
    private $message_queue;
    private $local_object;

    /**
     * Initializes a new server.
     *
     * @param mixed $redis_args Redis server arguments.
     * @param string $message_queue Name of Redis message queue.
     * @param mixed $local_object Handle to local wrapped object that will receive the RPC calls.
     */
    public function __construct($redis_args, $message_queue, $local_object) {
        $this->redis_args = $redis_args;
        $this->message_queue = $message_queue;
        $this->local_object = $local_object;
    }

    /**
     * Starts the server.
     */
    public function run() {
        $redis_pubsub_server = new Predis\Client($this->redis_args);
        $subscribers = $redis_pubsub_server->pubsub('numsub', $this->message_queue);
        if($subscribers[$this->message_queue] != 0){
            throw new \RuntimeException('Server already running for queue ' .
                $this->message_queue);
        }

        $pubsub = $redis_pubsub_server->pubSubLoop();

        $pubsub->subscribe($this->message_queue);
        foreach($pubsub as $message){
            # Pop a message from the queue.
            # Decode the message.
            # Check that the function exists.
            if($message->kind != 'message'){
                debug_print('Ignoring ' . $message->kind . ': ' . $message->payload);
                continue;
            }

            assert($message->channel == $this->message_queue);

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
                            get_class($e->getResponse());
                    }
                }
            }

            $message = json_encode($rpc_response);

            debug_print("RPC Response: $message");
            assert($redis_server->pubsub('numsub', $this->response_queue) > 0);
            $redis_server = new Predis\Client($this->redis_args);
            $redis_server->publish($response_queue, $message);
        }
    }

}

?>
