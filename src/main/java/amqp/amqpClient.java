package amqp;

import java.util.Properties;
import java.util.concurrent.TimeUnit;

import javax.annotation.PostConstruct;
import javax.jms.BytesMessage;
import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageListener;
import javax.jms.Session;
import javax.jms.TextMessage;
import javax.naming.Context;
import javax.naming.InitialContext;
import javax.naming.NamingException;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.stereotype.Component;


@Component
public class amqpClient {

private static final Logger logger = LogManager.getLogger(amqpClient.class);
	
	private static ConnectionFactory connfac = null;
	private static Connection con = null;
	private TimeUnit unit = null;
	private static MessageConsumer consumer = null;
	private boolean initial_startup = true;
	private static Properties env;
	private String Uuid = "";
	private static int MessageCount = 0;
	
	@PostConstruct
    public static void wub() {
        System.out.println( "Hello World!" );
        setup("TEST","");
    }	

	public static void setup(String queueName, String uuid) {
		logger.debug("Setting Properties");
		setProp(queueName, uuid);
		logger.debug("start Amqp Client");
		startClient();
	}

	static void setProp(String queueName, String uuid) {

		logger.info("Start Setting up the Client Properties!");
		env = new Properties();
		env.put(Context.INITIAL_CONTEXT_FACTORY, "org.apache.qpid.jms.jndi.JmsInitialContextFactory");
		logger.info("Setting INITIAL_CONTEXT_FACTORY " + env.getProperty("java.naming.factory.initial"));
		env.put("connectionfactory.myFactoryLookup", "amqp://localhost:5672");
//		logger.info("Setting connectionfactory.myFactoryLookup " + );
		env.put("queue.myEurocontroleLookup", queueName);
		logger.info("Setting queue.myQueueLookup " + env.getProperty("queue.myEurocontroleLookup"));
		env.put("queue.myEurocontroleQueueUUID", uuid);
		logger.info("Setting myQueueUUID " + env.getProperty("queue.myEurocontroleQueueUUID"));
//		initial_startup = false;
	}

	static void startClient() {
		try {
			Context ctx = new InitialContext(env);
//			SSLContext sslcontext = CustomSSLContextFactory.getContext();
			connfac = (ConnectionFactory) ctx.lookup("myFactoryLookup");
			org.apache.qpid.jms.JmsConnectionFactory connectionfac = (org.apache.qpid.jms.JmsConnectionFactory) connfac;
//			connectionfac.setSslContext(sslcontext);
			Destination EurocontroleDest = (Destination) ctx.lookup("myEurocontroleLookup");
			logger.debug("Destination set");
			con = connectionfac.createConnection();
			logger.debug("Connection created");
//			con.setExceptionListener(new ExceptionListener() {
//				public void exceptionThrown(Exception e) {
//					logger.error("Connection terminated ", e);				
//				}
//			});
			Session session = con.createSession(false, Session.AUTO_ACKNOWLEDGE);
			consumer = session.createConsumer(EurocontroleDest);
			consumer.setMessageListener(new MessageListener() {

				public void onMessage(Message message) {	
					logger.debug("Received Message in onMessage function of MessageConsumer. Ammount: " + MessageCount);
					MessageCount++;
					if (message instanceof BytesMessage){
						BytesMessage byteMessage = (BytesMessage) message;
//						sendProcessMessage(byteMessage);
					} else if(message instanceof TextMessage) {
						TextMessage txtMsg = (TextMessage) message;
						try {
							System.out.println(txtMsg.getText());
						} catch (JMSException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						}
//						sendProcessMessage(txtMsg);
					} else {
						logger.warn("Message not recognized as a TextMessage or BytesMessage.  It is of type: "+message.getClass().toString());
					}
				}
			});
			logger.info("attempting to start AMQP Connection");
			con.start();
			logger.info("Connection started");
			
			
		} catch (JMSException e) {		
			logger.error("Error in mein Amqp Client caught", e);

		} catch (NamingException e) {
			logger.error("Error in mein Amqp Client caught", e);
		}
	}
}
