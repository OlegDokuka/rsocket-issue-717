package com.rsocket.examples;

import io.rsocket.RSocketFactory;
import io.rsocket.client.LoadBalancedRSocketMono;
import io.rsocket.client.filter.RSocketSupplier;
import io.rsocket.frame.decoder.PayloadDecoder;
import io.rsocket.transport.netty.client.TcpClientTransport;
import io.rsocket.util.ByteBufPayload;
import org.apache.commons.lang3.SerializationUtils;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Hooks;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

import java.io.Serializable;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.stream.Collectors;

public class LoadTestClient {

    public static final int CALLS_COUNT = 1000000;

    public static void main(String[] args) throws InterruptedException {
        Hooks.onOperatorDebug();
        LoadTestClient loadTestClient = new LoadTestClient();
        loadTestClient.callLB();

    }

    private void callLB() throws InterruptedException {

        int[] server1 = new int[]{9000, 9001, 9000, 9001, 9000, 9001, 9000, 9001};

        Set<RSocketSupplier> suppliers = new HashSet<>();

        suppliers.addAll(Arrays.stream(server1)
                .mapToObj(port -> new RSocketSupplier(() -> {
                    System.err.println("!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!Happened!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!");
                    return Mono.just(
                            RSocketFactory
                                    .connect()
                                    .frameDecoder(PayloadDecoder.ZERO_COPY)
                                    .transport(TcpClientTransport.create("localhost", port))
                                    .start()
                                    .doOnSubscribe(s -> System.out.println("RSocket connection established." + s))
                            .block()
                    );
                }))
                .collect(
                        Collectors.toSet()));

        LoadBalancedRSocketMono balancer = LoadBalancedRSocketMono
                .create(Flux.just(suppliers));

        long start = new Date().getTime();

        int count = 8;
        CountDownLatch latch = new CountDownLatch(CALLS_COUNT * count);
        Flux.range(1, count)
                .parallel()
                .runOn(Schedulers.newElastic("test"))
                .subscribe(i -> thrGrp(balancer, latch), err -> {
                    System.err.println("In run test");
                    err.printStackTrace();
                });
        latch.await();
        System.out.println("Total Time taken for 10K records ---> (ms)" + (new Date().getTime() - start));
        Thread.sleep(100);
        System.exit(9);
    }

    private void thrGrp(LoadBalancedRSocketMono balancedRSocketMono, CountDownLatch latch) {
        System.out.println("Running test");
        Flux.range(1, CALLS_COUNT)
                .subscribe(i -> callServer(i, balancedRSocketMono, latch), err -> {
                    System.err.println("In thrGrp");
                    err.printStackTrace();
                });
    }

    private void callServer(int i, LoadBalancedRSocketMono balancedRSocketMono, CountDownLatch latch) {
        balancedRSocketMono
                .flatMap(rsocket -> {
                    return rsocket
                            .requestResponse(ByteBufPayload
                                    .create(SerializationUtils.serialize(getEmpData(i))));

                })
                .retry()
                .subscribe(payload -> {
                    String dataUtf8 = payload.getDataUtf8();
                    payload.release();
//                                System.out.println("Response Received for " + i + " in Client --> " + dataUtf8);
                    latch.countDown();
                }, err -> {
                    System.err.println("In callServer make a call");
                    err.printStackTrace();
                    try {
                        Thread.sleep(100);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                    System.exit(1);
                });
    }

    private EmployeeData getEmpData(int i) {
        return new EmployeeData("John" + i, "Doe", 22);
    }
}

class EmployeeData implements Serializable {

    private static final long serialVersionUID = 2892015151528050314L;

    String firstName;

    String lastName;

    int age;

    public EmployeeData(String firstName, String lastName, int age) {
        this.firstName = firstName;
        this.lastName = lastName;
        this.age = age;
    }

    public String getFirstName() {
        return firstName;
    }

    public void setFirstName(String firstName) {
        this.firstName = firstName;
    }

    public String getLastName() {
        return lastName;
    }

    public void setLastName(String lastName) {
        this.lastName = lastName;
    }

    public int getAge() {
        return age;
    }

    public void setAge(int age) {
        this.age = age;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;
        EmployeeData that = (EmployeeData) o;
        return age == that.age &&
                Objects.equals(firstName, that.firstName) &&
                Objects.equals(lastName, that.lastName);
    }

    @Override
    public int hashCode() {
        return Objects.hash(firstName, lastName, age);
    }
}