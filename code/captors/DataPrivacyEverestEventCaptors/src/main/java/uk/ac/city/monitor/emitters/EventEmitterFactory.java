package uk.ac.city.monitor.emitters;

import uk.ac.city.monitor.enums.EmitterType;

import java.util.Properties;

public class EventEmitterFactory {

    private EventEmitterFactory EventEmitterFactory(){
        return new EventEmitterFactory();
    }

    public static Emitter getInstance(EmitterType type, Properties props){

        Emitter emitter = null;

        switch (type){
            case RABBITMQ:
                emitter = new RabbitMQEmitter(props);
                break;
            case SOCKET:
                emitter = new SocketEmitter(props);
                break;
            case XMPP:
                break;
            default:
                emitter = new RabbitMQEmitter(props);
        }

        return emitter;
    }
}
