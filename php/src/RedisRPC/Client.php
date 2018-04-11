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
use RedisRPC\RemoteException;

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

# Ref: http://stackoverflow.com/questions/2257441
# Ref: http://stackoverflow.com/questions/853813/how-to-create-a-random-string-using-php
/**
 *
 */
function random_string($size, $valid_chars='ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789') {

    // start with an empty random string
    $retval = "";

    // count the number of chars in the valid chars string so we know how many choices we have
    $num_valid_chars = strlen($valid_chars);

    // repeat the steps until we've created a string of the right size
    for ($i = 0; $i < $size; $i++)
    {
        // pick a random number from 1 up to the number of valid chars
        $random_pick = mt_rand(1, $num_valid_chars);

        // take the random character out of the string of valid chars
        // subtract 1 from $random_pick because strings are indexed starting at 0, and we started picking at 1
        $random_char = $valid_chars[$random_pick-1];

        // add the randomly-chosen char onto the end of our string so far
        $retval .= $random_char;
    }

    // return our finished random string
    return $retval;
}

/**
 *
 */
class Client {

    private $redis_args;
    private $redis_server;
    private $redis_pubsub_server;
    private $message_queue;

    public function __construct($redis_args, $message_queue) {
        $this->redis_server = null;
        $this->redis_pubsub_server = null;
        $this->redis_args = $redis_args;
        $this->message_queue = $message_queue;
    }

    public function __destruct(){
        if($this->redis_server){
            $this->redis_server->disconnect();
        }
        if($this->redis_pubsub_server){
            $this->redis_pubsub_server->disconnect();
        }
    }

    public function __call($name, $arguments) {
        # Construct the RPC Request message from the $name and $arguments.
        # Send the RPC Request to Redis.
        # Block on the RPC Response from Redis.
        # Extract the return value.
        $response_queue = $this->message_queue . ':rpc:' . random_string(8);
        $rpc_request = array(
            'function_call' => array(
                'name' => $name,
                'args' => $arguments,
            ),
            'response_queue' => $response_queue
        );
        $request = json_encode($rpc_request);
        debug_print("RPC Request: $request");

        $this->redis_server = new Predis\Client($this->redis_args);
        $this->redis_pubsub_server = new Predis\Client($this->redis_args);
        $pubsub = $this->redis_pubsub_server->pubSubLoop();

        $message_queue = $this->message_queue . ':server';
        $subscribers = $this->redis_server->pubsub('numsub', $message_queue);
        if($subscribers[$message_queue] == 0){
            throw new \RuntimeException('No servers available for queue ' .
                $message_queue);
        }

        $pubsub->subscribe($response_queue);
        $this->redis_server->publish($message_queue, $request);
        $this->redis_server->disconnect();

        $response = null;
        foreach($pubsub as $message){
            if($message->kind == 'message'){
                assert($message->channel == $response_queue);
                $response = $message;
                $pubsub->unsubscribe($response_queue);
                unset($pubsub);
                $this->redis_pubsub_server->disconnect();
            }
            debug_print('Ignoring ' . $message->kind . ': ' . $message->payload);
        }

        debug_print('RPC Response: ' . $response->payload . PHP_EOL);
        $rpc_response = json_decode($response->payload);
        if (array_key_exists('exception', $rpc_response) && $rpc_response->exception != NULL) {
            if(array_key_exists('exception_type', $rpc_response)){
                $exception_type = $rpc_response->exception_type;
                $exception = new $exception_type($rpc_response->exception);

                /* Convert json object to response object */
                $response = new $rpc_response->response_type();
			    foreach($rpc_response->response as $key => $value){
				    $response->{$key} = $value;
			    }

                $exception->setResponse($response);
                throw $exception;
            }else{
                throw new \RuntimeException($rpc_response->exception);
            }
        }
        if (!array_key_exists('return_value',$rpc_response)) {
            throw new RemoteException('Malformed RPC Response message');
        }
        print_r($rpc_response);
        if(array_key_exists('return_type', $rpc_response)
                && $rpc_response->return_type){
            return $rpc_response->return_type($rpc_response->return_value);
        }else{
            return $rpc_response->return_value;
        }
    }
}

?>
