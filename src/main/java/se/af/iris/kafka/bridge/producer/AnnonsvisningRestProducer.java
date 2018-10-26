package se.af.iris.kafka.bridge.producer;

import org.apache.avro.specific.SpecificRecordBase;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.*;
import org.springframework.web.client.RestTemplate;
import se.af.iris.kafka.bridge.util.JsonAvroMessage;
import se.arbetsformedlingen.kafka.Annonsvisning;
import se.arbetsformedlingen.kafka.Egenskap;
import se.arbetsformedlingen.kafka.Matchningsprofil;
import se.arbetsformedlingen.kafka.Profilkriterie;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class AnnonsvisningRestProducer {

    private static final Logger log = LoggerFactory.getLogger(AnnonsvisningRestProducer.class);

    private static final RestTemplate restTemplate = new RestTemplate();

    //private static final String url = "http://sauron.ws.ams.se:8082/topics/test-avro-topic";
    private static final String url = "http://omegateam.se:8082/topics/test_annonsvisningar";

    public AnnonsvisningRestProducer() {
        System.out.println(url);

    }

    public static void main(String args[]) throws InterruptedException, IOException {

        AnnonsvisningRestProducer omegaRestClient = new AnnonsvisningRestProducer();
        JsonAvroMessage jsonMessage = new JsonAvroMessage(genereraAnnonsvisningar(10));

        omegaRestClient.sendMessage(jsonMessage.getJsonMessage());

        /*
        for (int i = 0; i < 10; i++) {
            omegaRestClient.sendMessage(restTemplate, url, jsonMessage.getJsonMessage());
            Thread.sleep(100);
        }
        */
    }

    public String sendMessage(SpecificRecordBase record) {

        JsonAvroMessage jsonMessage = new JsonAvroMessage(record);

        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.valueOf("application/vnd.kafka.avro.v2+json"));
        HttpEntity<String> entity = new HttpEntity<String>(jsonMessage.getJsonMessage(), headers);
        String response = restTemplate.postForObject(url, entity, String.class);
        return response;

    }

    public String sendMessage(String jsonMessage) {
        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.valueOf("application/vnd.kafka.avro.v2+json"));
        HttpEntity<String> entity = new HttpEntity<String>(jsonMessage, headers);
        String response = restTemplate.postForObject(url, entity, String.class);
        return response;
    }

    private static List<SpecificRecordBase> genereraAnnonsvisningar(int antal) {
        List<SpecificRecordBase> annonsvisningar = new ArrayList<>();
        for (int i = 0; i < antal; i++) {
            annonsvisningar.add(genereraAnvisning());
        }
        return annonsvisningar;
    }

    private static Annonsvisning genereraAnvisning() {
        Annonsvisning annonsvisning = Annonsvisning.newBuilder()
                .setDeviceId("7777777")
                .setSessionId("123456")
                .setAnnonsId("234567")
                .setTidpunkt(System.currentTimeMillis())
                .setAnvId("1234")
                .setMatchningsprofil(generaMatchningsprofil())
                .build();
        return annonsvisning;
    }

    private static Matchningsprofil generaMatchningsprofil() {
        List<Profilkriterie> profilkriterier = new ArrayList<Profilkriterie>();
        profilkriterier.add(Profilkriterie.newBuilder()
                .setTyp("FRITEXT").setVarde("fritext")
                .setEgenskaper(new ArrayList<Egenskap>()).build());
        Matchningsprofil matchningsprofil = Matchningsprofil.newBuilder().setProfilkriterier(profilkriterier).build();
        return matchningsprofil;
    }

}
