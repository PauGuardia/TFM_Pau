from azure.eventhub import EventHubConsumerClient
import pandas as pd
import json
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart

def get_messages() :
    print("empiezo a ejecutar")
    connection_str = 'Endpoint=sb://ihsuprodamres031dednamespace.servicebus.windows.net/;SharedAccessKeyName=RemoteRead;SharedAccessKey=J5vbl526r0p0l+6x0/15dicj4FGfLYrIObHVdNX6n8k=;EntityPath=iothub-ehub-logistics-24712225-fb01c8ea40'
    #connection_str= 'HostName=Logistics.azure-devices.net;SharedAccessKeyName=RemoteRead;SharedAccessKey=J5vbl526r0p0l+6x0/15dicj4FGfLYrIObHVdNX6n8k='
    consumer_group = '$Default'
    eventhub_name = 'iothub-ehub-logistics-24712225-fb01c8ea40'
    client = EventHubConsumerClient.from_connection_string(connection_str, consumer_group, eventhub_name=eventhub_name)
    destinatario_mail="no.replly.temperatura.tfm@gmail.com"
    asunto_mail = "[ALERTA] Temperatura en camión baja"
    mensaje_mail = "Temperatura baja detectada en camión."
    remitente_mail = "guardiapau@gmail.com"
    contraseña_remitente = "thcjjyvtipswfxok"


    def on_event_batch(partition_context, events):
        #Checking whether there is any event returned as we have set max_wait_time
        if(len(events) == 0):
        #closing the client if there is no event triggered.
            client.close()
        else:
            for event in events:
                #Event.body operation
                mensaje_mail = "Temperatura baja detectada en camión." 
                device_id_hub = event._raw_amqp_message._annotations[b'iothub-connection-device-id']
                device_ids  = str(device_id_hub).split("'")
                device_id = device_ids[1]
                body_event=event.body_as_str()
                data_temperature= json.loads(body_event)
                ambient_temperature = data_temperature["ambient"]["temperature"]
                if (ambient_temperature < 20.7):
                    mensaje_mail = mensaje_mail + " Temperatura: " + str(ambient_temperature) + " en el camión "+ str(device_id) + """ .
Por favor, contactar con conductor para proporcionar indicaciones."""
                    enviar_correo(destinatario_mail, asunto_mail, mensaje_mail, remitente_mail, contraseña_remitente)
    with client:
        client.receive_batch(
            on_event_batch=on_event_batch,
            starting_position="-1", max_wait_time = 10, max_batch_size=2  # "-1" is from the beginning of the partition.
            #Max_wait_time - no activitiy for that much - call back function is called with No events.
        )
    return None

def enviar_correo(destinatario, asunto, mensaje, remitente, contraseña):
    # Configura los parámetros del servidor SMTP de tu proveedor de correo electrónico
    smtp_server = "smtp.gmail.com"
    smtp_port = 587  # Puerto SMTP típico para envío de correo seguro (TLS)

    # Crea un objeto SMTP
    server = smtplib.SMTP(smtp_server, smtp_port)
    server.starttls()  # Inicia una conexión segura

    try:
        # Inicia sesión en tu cuenta de correo electrónico
        server.login(remitente, contraseña)

        # Crea el mensaje
        mensaje_correo = MIMEMultipart()
        mensaje_correo["From"] = remitente
        mensaje_correo["To"] = destinatario
        mensaje_correo["Subject"] = asunto

        # Agrega el mensaje al cuerpo del correo
        mensaje_correo.attach(MIMEText(mensaje, "plain"))

        # Envía el correo
        server.sendmail(remitente, destinatario, mensaje_correo.as_string())
        print("Correo enviado exitosamente")

    except Exception as e:
        print("Error al enviar el correo:", str(e))
    finally:
        # Cierra la conexión con el servidor SMTP
        server.quit()

get_messages()
destinatario_mail="no.replly.temperatura.tfm@gmail.com"
asunto_mail = "[ALERTA] Temperatura en camión baja"
mensaje_mail = "Temperatura baja detectada en camión."
remitente_mail = "guardiapau@gmail.com"
contraseña_remitente = "thcjjyvtipswfxok"
#enviar_correo(destinatario_mail, asunto_mail, mensaje_mail, remitente_mail, contraseña_remitente)

