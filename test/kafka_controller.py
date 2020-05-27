"""
A controller for Kafka useful for running tests.

Production use is not recommended.
"""
from pathlib import Path
from core.test_utils import TestException, find_free_port
import os
import tempfile
import subprocess
import time
import shutil

from kafka import KafkaProducer, KafkaConsumer


class KafkaController:
    """
    The main Kafka controller class.

    Attributes:
    port - the port for the Kafka service.
    temp_dir - the location of the Kafka data and logs.
    producer - a kafka-python producer pointed at the server.
    """

    def __init__(self, zookeeperexe: Path, kafkaexe: Path, root_temp_dir: Path) -> None:
        '''
        Create and start a new Kafka server. An unused port will be selected for the server and
        for zookeeper.

        :param zookeeperexe: The path to the Zookeeper server executable to run.
        :param kafkaexe: The path to the Kafka server executable to run.
        :param root_temp_dir: A temporary directory in which to store Kafka data and log files.
            The files will be stored inside a child directory that is unique per invocation.
        '''
        zookeeperexe = Path(os.path.expanduser(zookeeperexe))
        kafkaexe = Path(os.path.expanduser(kafkaexe))
        if not zookeeperexe or not os.access(zookeeperexe, os.X_OK):
            raise TestException('zookeeper executable path {} does not exist or is not executable.'
                                .format(zookeeperexe))
        if not kafkaexe or not os.access(kafkaexe, os.X_OK):
            raise TestException('kafka executable path {} does not exist or is not executable.'
                                .format(kafkaexe))
        if not root_temp_dir:
            raise ValueError('root_temp_dir is None')

        # make temp dirs
        root_temp_dir = root_temp_dir.absolute()
        self.temp_dir = Path(tempfile.mkdtemp(prefix='KafkaController-', dir=str(root_temp_dir)))
        zoo_dir = self.temp_dir.joinpath('zookeeper')
        os.makedirs(zoo_dir, exist_ok=True)
        kafka_dir = self.temp_dir.joinpath('kafka')
        os.makedirs(kafka_dir, exist_ok=True)

        zooport = find_free_port()

        self._zoo_proc, self._zoo_out = self._start_zoo(zookeeperexe, zooport, zoo_dir)

        self.port = find_free_port()

        self._kafka_proc, self._kafka_out = self._start_kafka(
            kafkaexe, self.port, zooport, kafka_dir)

        self.producer = KafkaProducer(bootstrap_servers=[f'localhost:{self.port}'])
        # check kafka is up
        KafkaConsumer(bootstrap_servers=[f'localhost:{self.port}']).topics()

    def _start_zoo(self, exe, port, zoo_dir):
        datadir = zoo_dir.joinpath('data')
        os.makedirs(datadir, exist_ok=True)
        configfile = zoo_dir.joinpath('zoo.cfg')
        with open(configfile, 'w') as c:
            # this is the default config for zookeeper provided in the kafka tarball
            c.write(f'dataDir={datadir}\n')
            c.write(f'clientPort={port}\n')
            c.write('maxClientCnxns=0\n')
            c.write('admin.enableServer=false')

        command = [str(exe), str(configfile)]

        self._outfile = open(zoo_dir.joinpath('zoo.out'), 'w')
        self._proc = subprocess.Popen(command, stdout=self._outfile, stderr=subprocess.STDOUT)
        time.sleep(1)  # wait for server to start up
        return self._proc, self._outfile

    def _start_kafka(self, exe, port, zooport, kafka_dir):
        logdir = kafka_dir.joinpath('logs')
        configfile = kafka_dir.joinpath('kafka.cfg')
        with open(configfile, 'w') as c:
            # this is the default config for kafka provided in the kafka tarball
            c.write('broker.id=0\n')
            c.write(f'listeners=PLAINTEXT://localhost:{port}\n')
            c.write('num.network.threads=3\n')
            c.write('num.io.threads=8\n')
            c.write('socket.send.buffer.bytes=102400\n')
            c.write('socket.receive.buffer.bytes=102400\n')
            c.write('socket.request.max.bytes=104857600\n')
            c.write(f'log.dirs={logdir}\n')  # this is the data directory
            c.write('num.partitions=1\n')
            c.write('num.recovery.threads.per.data.dir=1\n')
            c.write('offsets.topic.replication.factor=1\n')
            c.write('transaction.state.log.replication.factor=1\n')
            c.write('transaction.state.log.min.isr=1\n')
            c.write('log.retention.hours=168\n')
            c.write('log.segment.bytes=1073741824\n')
            c.write('log.retention.check.interval.ms=300000\n')
            c.write(f'zookeeper.connect=localhost:{zooport}\n')
            c.write('zookeeper.connection.timeout.ms=1000\n')
            c.write('group.initial.rebalance.delay.ms=0\n')

            # this is additional config
            c.write('delete.topic.enable=true\n')

        command = [str(exe), str(configfile)]

        self._outfile = open(kafka_dir.joinpath('kafka.out'), 'w')
        self._proc = subprocess.Popen(command, stdout=self._outfile, stderr=subprocess.STDOUT)
        time.sleep(3)  # wait for server to start up
        return self._proc, self._outfile

    def get_consumer(self, topic):
        return KafkaConsumer(topic, bootstrap_servers=[f'localhost:{self.port}'])

    def destroy(self, delete_temp_files: bool) -> None:
        """
        Shut down the Kafka server.

        :param delete_temp_files: delete all the Kafka data files and logs generated during the
            test.
        """
        if self._kafka_proc:
            self._kafka_proc.terminate()
        if self._zoo_proc:
            self._zoo_proc.terminate()
        if self._kafka_out:
            self._kafka_out.close()
        if self._zoo_out:
            self._zoo_out.close()
        if delete_temp_files and self.temp_dir:
            shutil.rmtree(self.temp_dir)

    # TODO CLEAR KAFKA topic https://stackoverflow.com/a/30833979/643675


def main():
    zooexe = Path('~/kafka/kafka_2.12-2.5.0/bin/zookeeper-server-start.sh')
    kafkaexe = Path('~/kafka/kafka_2.12-2.5.0/bin/kafka-server-start.sh')

    kc = KafkaController(zooexe, kafkaexe, Path('./test_temp_can_delete'))
    print(f'port: {kc.port}')
    print(f'temp_dir: {kc.temp_dir}')
    kc.producer.send('mytopic', 'some message'.encode('utf-8'))

    cons = KafkaConsumer(
        'mytopic', bootstrap_servers=[f'localhost:{kc.port}'], auto_offset_reset='earliest')
    print(cons.poll(timeout_ms=1000))
    input('press enter to shut down')
    kc.destroy(True)


if __name__ == '__main__':
    main()
