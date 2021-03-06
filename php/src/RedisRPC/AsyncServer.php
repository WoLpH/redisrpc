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
class AsyncServer {

    private $pubsub;
    private $redis_args;
    private $redis_pubsub_server;
    private $local_objects;

    /**
     * Initializes a new server.
     *
     * @param mixed $redis_args Redis server arguments.
     * @param mixed $local_object Handle to local wrapped objects as
     *        associative array (key = queue name) that will receive the RPC calls.
     */
    public function __construct($redis_args, &$local_objects) {
        $this->pubsub = null;
        $this->redis_args = $redis_args;
        $this->redis_pubsub_server = null;
        $this->local_objects = &$local_objects;
    }

    public function __destruct(){
        unset($this->pubsub);
        if($this->redis_pubsub_server){
            $this->redis_pubsub_server->disconnect();
        }
    }

    public function add_local_object($key, $value){
        $this->local_objects[$key] = $value;
        $this->pubsub->subscribe($key . ':server');
    }

    public function remove_local_object($key){
        $this->pubsub->unsubscribe($key . ':server');
        unset($this->local_objects[$key]);
    }

    public function get_local_objects(){
        return array_keys($this->local_objects);
    }

    private async function pubsub_generator(): AsyncIterator<array>{
        foreach($this->pubsub await as $message){
            yield $message;
        }
    }

    /**
     * Starts the server.
     */
    public async function run() {
        $this->redis_pubsub_server = new Predis\Client($this->redis_args);
        $this->pubsub = $this->redis_pubsub_server->asyncPubSubLoop();

        $started = 0;
        $redis_server = new Predis\Client($this->redis_args);
        foreach($this->local_objects as $key => $local_object){
            $message_queue = $key . ':server';
            $subscribers = $redis_server->pubsub('numsub', $message_queue);
            if($subscribers[$message_queue] != 0){
                echo 'Server already running for ' . $message_queue . PHP_EOL;
            }else{
                $this->pubsub->subscribe($message_queue);
                $started++;
            }
        }
        unset($redis_server);
        if($started == 0){
            throw new \RuntimeException('Server already running for queues' .
                implode(', ', array_keys($this->local_objects)));
        }

        $generator = $this->pubsub->generator();
        foreach($generator await as $message){
            if($message->kind != 'message'){
                # debug_print('Ignoring ' . $message->kind . ': ' .
                #     $message->payload);
                continue;
            }

            await $this->run_message($message);
        }
    }

    public async function run_message($message){
        # Pop a message from the queue.
        # Decode the message.
        # Check that the function exists.
        #
        // assert($message->channel == $this->message_queue);
        $message_queue = substr($message->channel, 0, -7);
        $local_object = $this->local_objects[$message_queue];

        $rpc_request = json_decode($message->payload);
        $response_queue = $rpc_request->response_queue;
        $function_call = FunctionCall::from_object(
            $rpc_request->function_call);
        $code = $function_call->name;
        $code .= '(' . json_encode($function_call->args) . ')';
        debug_print('RPC Request: ' . $code);

        if (!method_exists($local_object, $function_call->name)) {
            $rpc_response = array(
                'exception' => 'method "' . $function_call->name . 
                '" does not exist');
        }else{
            try {
                $response_value = await call_user_func_array(
                    array($local_object, $function_call->name),
                    $function_call->args);

                $rpc_response = array(
                    'return_value' => $response_value,
                    'return_type' => gettype($response_value) == 'object'
                    ? get_class($response_value) : gettype($response_value),
                );
            }catch (\Throwable $e) {
                echo $e->getTraceAsString() . "\n";
                $rpc_response = array(
                    'exception' => $e->getMessage(),
                    'exception_trace' => $e->getTraceAsString(),
                    'exception_type' => gettype($e) == 'object'
                    ? get_class($e) : gettype($e),
                );
                if(method_exists($e, 'getResponse')){
                    $rpc_response['return_value'] = $e->getResponse();
                    $rpc_response['return_type'] =
                        gettype($e->getResponse()) == 'object'
                        ? get_class($e->getResponse())
                        : gettype($e->getResponse());
                }
            }
        }

        $message = json_encode($rpc_response);

        debug_print("RPC Response: $message");
        $redis_server = new Predis\Client($this->redis_args);
        $redis_server->publish($response_queue, $message);
        unset($redis_server);
        gc_collect_cycles();
    }
}
