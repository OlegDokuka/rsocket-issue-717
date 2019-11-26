package com.rsocket.examples;

import io.netty.buffer.ByteBuf;
import io.rsocket.*;
import io.rsocket.frame.decoder.PayloadDecoder;
import io.rsocket.transport.netty.server.TcpServerTransport;
import io.rsocket.util.ByteBufPayload;
import io.rsocket.util.DefaultPayload;
import org.apache.commons.lang3.SerializationUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.time.FastDateFormat;
import reactor.core.publisher.Mono;

import java.util.Arrays;
import java.util.Date;

public class LoadTestServers {

    static final String HOST = "localhost";

    //    static Logger log = Logger.getLogger(LoadTestServers.class.getName());

    static final int[] PORTS = new int[] {9000, 9001};

    public static void main(String[] args) throws InterruptedException {

        Arrays.stream(PORTS)
                .forEach(port -> RSocketFactory.receive()
                        .frameDecoder(PayloadDecoder.ZERO_COPY)
                        .acceptor(new SimpleSocketAcceptor("SERVER-" + port))
                        .transport(TcpServerTransport.create(HOST, port))
                        .start()
                        .subscribe());

        System.out.println("Servers running");

        Thread.currentThread().join();
    }

    static class SimpleSocketAcceptor implements SocketAcceptor {

        private String serverName;

        SimpleSocketAcceptor(String serverName) {
            this.serverName = serverName;
        }

        @Override
        public Mono<RSocket> accept(ConnectionSetupPayload setup, RSocket sendingSocket) {
            System.out.println("Received setup connection on acceptor: [{}]" + serverName);
            return Mono.just(new AbstractRSocket() {
                @Override
                public Mono<Payload> requestResponse(Payload payload) {
                    ByteBuf dataByteBuf = payload.sliceData();
                    byte[] dataBytes = new byte[dataByteBuf.readableBytes()];
                    dataByteBuf.getBytes(dataByteBuf.readerIndex(), dataBytes);
                    EmployeeData employeeData = SerializationUtils.deserialize(dataBytes);
                    String response = StringUtils.join(employeeData.getFirstName(), "response at ->",
                            FastDateFormat.getInstance("MM-dd-yyyy").format(new Date()));
                    payload.release();
                    return Mono.just(ByteBufPayload.create(response));
                }
            });
        }
    }
}