package com.example.demo;

import com.mongodb.MongoClient;
import com.mongodb.ServerAddress;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.opencsv.CSVReader;
import com.opencsv.exceptions.CsvException;
import org.bson.Document;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.io.FileReader;
import java.io.IOException;
import java.util.*;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collector;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class CSVUtilTest {

    @Test
    void converterData(){
        List<Player> list = CsvUtilFile.getPlayers();
        assert list.size() == 18207;
    }

    @Test
    void stream_filtrarJugadoresMayoresA35(){
        List<Player> list = CsvUtilFile.getPlayers();
        Map<String, List<Player>> listFilter = list.parallelStream()
                .filter(player -> player.age >= 35)
                .map(player -> {
                    player.name = player.name.toUpperCase(Locale.ROOT);
                    return player;
                })
                .flatMap(playerA -> list.parallelStream()
                        .filter(playerB -> playerA.club.equals(playerB.club))
                )
                .distinct()
                .collect(Collectors.groupingBy(Player::getClub));

        assert listFilter.size() == 322;
    }


    @Test
    void reactive_filtrarJugadoresMayoresA35(){
        List<Player> list = CsvUtilFile.getPlayers();
        Flux<Player> listFlux = Flux.fromStream(list.parallelStream()).cache();
        Mono<Map<String, Collection<Player>>> listFilter = listFlux
                .filter(player -> player.age >= 35)
                .map(player -> {
                    player.name = player.name.toUpperCase(Locale.ROOT);
                    return player;
                })
                .buffer(100)
                .flatMap(playerA -> listFlux
                         .filter(playerB -> playerA.stream()
                                 .anyMatch(a ->  a.club.equals(playerB.club)))
                )
                .distinct()
                .collectMultimap(Player::getClub);

        assert listFilter.block().size() == 322;
    }

    @Test
    void conectar_MongoDB(){
        MongoClient mongoClient = new MongoClient(new ServerAddress("localhost",27017));
        MongoDatabase mongoDatabase = mongoClient.getDatabase("Jugadores");
        MongoCollection<Document> collectionPlayers = mongoDatabase.getCollection("JugadoresIngresados");
        List<Player> list = CsvUtilFile.getPlayers();
        Flux<Player> listFlux = Flux.fromStream(list.parallelStream()).cache();
        Player playerOne = list.get(0);
        collectionPlayers.insertOne(new Document("data", playerOne.toString()));
        /*
        List<Document> documents = new ArrayList<>();
        Map<String, String> documentsMap = new HashMap<>();
        documentsMap.put(listFlux.buffer()
                .subscribe(Player::getName), listFlux.buffer()
                .subscribe(Player::toString));
        //documentsMap.put(listFlux.subscribe(Player::getName), listFlux.subscribe(Player::toString));
        //collectionPlayers.in
        Collection<Document> collection = new ArrayList<Document>(documents);

         */

        /*
        //collection.addAll()
        documents.addAll(listFlux
                .filter(Objects::nonNull)
                .map(player -> player.toString())
                .collectList(Collectors.toList()));
        List<Document> documents = Document.parse(listFlux.
                filter(Objects::nonNull).map(player -> player.toString()).toString());
        collectionPlayers.insertMany(documents);

         */

    }

    @Test
    void filtrarPorEquipo(){
        List<Player> list = CsvUtilFile.getPlayers();
        Flux<Player> listFlux = Flux.fromStream(list.parallelStream()).cache();
        Mono<List<Player>> listFilter = listFlux
                //.filter(Objects::nonNull)
                .filter(player -> player.club.contains("Real Madrid"))
                .collect(Collectors.toList());

        System.out.println("Jugadores del Real Madrid: " + listFilter.block().size() + "\n");

        //listFilter.block().forEach(System.out::println);
    }

    @Test
    void consultarNacionalidad(){
        List<Player> list = CsvUtilFile.getPlayers();
        Flux<Player> listFlux = Flux.fromStream(list.parallelStream()).cache();
        Mono<List<Player>> listFilter = listFlux
                .filter(player -> player.getNational().contains("Spain"))
                .collect(Collectors.toList());

        System.out.println("Jugadores de España: " + listFilter.block().size() + "\n");
        listFilter.block().forEach(System.out::println);
    }

}
