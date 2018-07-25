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
    private $redis_url;
    private $redis_args;
    private $local_objects;

    /**
     * Initializes a new server.
     *
     * @param mixed $redis_url Redis server arguments.
     * @param mixed $redis_args Redis server arguments.
     * @param mixed $local_object Handle to local wrapped objects as
     *        associative array (key = queue name) that will receive the RPC calls.
     */
    public function __construct($redis_url, $redis_args) {
        $this->pubsub = null;
        $this->redis_url = $redis_url;
        $this->redis_args = $redis_args;
        $this->local_objects = array();
        $this->connect();

        $this->last_update = 0;
        $this->queue_keys = array();
        $this->activity_keys = array();
    }

    public function connect(){
        $this->redis_server = new Predis\Client($this->redis_url, $this->redis_args);
    }

    public function get_running_key($queue){
        return $queue . ':running';
    }

    public function get_queue_key($queue){
        return $queue . ':queue';
    }

    public function get_activity_key($queue){
        return $queue . ':activity';
    }

    public function get_rpc_key($queue){
        return $queue . ':rpc';
    }

    public function add_local_object($key, $value){
        $queue = $this->get_queue_key($key);
        $this->local_objects[$key] = $value;
        $this->update_keys();
    }

    public function update_keys(){
        $this->queue_keys = array();
        $this->activity_keys = array();
        foreach($this->local_objects as $key => $value){
            $this->queue_keys[] = $this->get_queue_key($key);
            $this->activity_keys[] = $this->get_activity_key($key);
        }
    }

    public function remove_local_object($key){
        unset($this->local_objects[$key]);
        $this->update_keys();
    }

    public function get_local_objects(){
        return array_keys($this->local_objects);
    }

    public function set_active(){
        $now = time();

        if($now - $this->last_update > 5){
            $this->last_update = $now;
            $pipe = $this->redis_server->pipeline();
            foreach($this->activity_keys as $key){
                $pipe->setex($key, 300, $now);
            }
            $pipe->execute();
        }
    }

    /**
     * Starts the server.
     */
    public function run() {
        $this->update_keys();
        $this->set_active();
        try{
            while(true){
                list($name, $message) = $this->redis_server->blpop(
                    $this->queue_keys, 5);

                $this->set_active();

                if(!empty($name)){
                    $name = implode(':', explode(':', $name, -1));
                    $this->run_message($name, $message);
                }
            }
        }catch(\Predis\CommunicationException $e){
            echo "Redis disconnected: $e, reconnecting";
            echo $e->getTraceAsString();
            return $this->run();
        }finally{
            $pipe = $this->redis_server->pipeline();
            foreach($this->activity_keys as $key){
                $this->redis_server->del($key);
            }
            $pipe->execute();
		}
    }

    public function run_message($name, $message){
        $local_object = $this->local_objects[$name];

        $rpc_request = json_decode($message);
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
                $response_value = call_user_func_array(
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
        $pipe = $this->redis_server->pipeline();
        $this->redis_server->lpush($response_queue, $message);
        $this->redis_server->expire($response_queue, 300);
        $pipe->execute();
        gc_collect_cycles();
    }
}

?>
