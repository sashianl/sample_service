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
from kafka.errors import NoBrokersAvailable


class KafkaController:
    """
    The main Kafka controller class.

    Attributes:
    port - the port for the Kafka service.
    temp_dir - the location of the Kafka data and logs.
    producer - a kafka-python producer pointed at the server.
    """

    _ZOO_EXE = 'zookeeper-server-start.sh'
    _KAFKA_EXE = 'kafka-server-start.sh'
    _KAFKA_TOPIC_EXE = 'kafka-topics.sh'

    def __init__(self, kafka_bin_dir: Path, root_temp_dir: Path) -> None:
        '''
        Create and start a new Kafka server. An unused port will be selected for the server and
        for zookeeper.

        :param kafka_bin_dir: The path to the Kafka bin dir containing the Zookeeper and Kafka
            shell scripts.
        :param root_temp_dir: A temporary directory in which to store Kafka data and log files.
            The files will be stored inside a child directory that is unique per invocation.
        '''
        self._bin_dir = Path(os.path.expanduser(kafka_bin_dir))
        zookeeperexe = self._bin_dir.joinpath(self._ZOO_EXE)
        kafkaexe = self._bin_dir.joinpath(self._KAFKA_EXE)
        topicsexe = self._bin_dir.joinpath(self._KAFKA_TOPIC_EXE)
        self._check_exe(zookeeperexe)
        self._check_exe(kafkaexe)
        self._check_exe(topicsexe)
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

        self._zooport = find_free_port()

        self._zoo_proc, self._zoo_out = self._start_zoo(zookeeperexe, self._zooport, zoo_dir)

        self.port = find_free_port()

        self._kafka_proc, self._kafka_out = self._start_kafka(
            kafkaexe, self.port, self._zooport, kafka_dir)
        self.producer = KafkaProducer(bootstrap_servers=[f'localhost:{self.port}'])

    def _check_exe(self, exe):
        if not exe or not os.access(exe, os.X_OK):
            raise TestException(f'executable path {exe} does not exist or is not executable.')

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

        outfile = open(zoo_dir.joinpath('zoo.out'), 'w')
        proc = subprocess.Popen(command, stdout=outfile, stderr=subprocess.STDOUT)
        time.sleep(1)  # wait for server to start up
        return proc, outfile

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

        outfile = open(kafka_dir.joinpath('kafka.out'), 'w')
        proc = subprocess.Popen(command, stdout=outfile, stderr=subprocess.STDOUT)

        for count in range(40):
            err = None
            time.sleep(1)  # wait for server to start
            try:
                KafkaProducer(bootstrap_servers=[f'localhost:{port}'])
                break
            except NoBrokersAvailable as e:
                err = TestException('No Kafka brokers available')
                err.__cause__ = e
        if err:
            self._print_kafka_logs()
            self._print_logs(outfile, 'Kafka', True)
            raise err
        self.startup_count = count + 1
        return proc, outfile

    def clear_topic(self, topic: str):
        """
        Remove all records from a topic.

        Note this takes about 2 seconds.

        :param topic: the topic to clear.
        """
        exe = self._bin_dir.joinpath(self._KAFKA_TOPIC_EXE)
        command = [
            str(exe), '--zookeeper',  f'localhost:{self._zooport}', '--delete', '--topic', topic]
        # just let any exceptions raise
        subprocess.run(command, capture_output=True, check=True)

    def clear_all_topics(self):
        """
        Remove all records from all topics.

        Note this takes about 2 seconds per topic.
        """
        cons = KafkaConsumer(bootstrap_servers=[f'localhost:{self.port}'], group_id='foo')
        for topic in cons.topics():
            self.clear_topic(topic)

    def destroy(self, delete_temp_files: bool = True, dump_logs_to_stdout: bool = False) -> None:
        """
        Shut down the Kafka server.

        :param delete_temp_files: delete all the Kafka data files and logs generated during the
            test.
        """
        if self._kafka_proc:
            self._kafka_proc.terminate()
        if self._zoo_proc:
            self._zoo_proc.terminate()
        self._print_kafka_logs(dump_logs_to_stdout=dump_logs_to_stdout)
        if self._kafka_out:
            self._kafka_out.close()
        if self._zoo_out:
            self._zoo_out.close()
        if delete_temp_files and self.temp_dir:
            shutil.rmtree(self.temp_dir)

    # closes logfile
    def _print_kafka_logs(self, dump_logs_to_stdout=True):
        self._print_logs(self._zoo_out, 'Zookeeper', dump_logs_to_stdout)
        self._print_logs(self._kafka_out, 'Kafka', dump_logs_to_stdout)

    def _print_logs(self, file_, name, dump_logs_to_stdout):
        if file_:
            file_.close()
            if dump_logs_to_stdout:
                print(f'\n{name} logs:')
                with open(file_.name) as f:
                    for line in f:
                        print(line)


def main():
    bindir = Path('~/kafka/kafka_2.12-2.5.0/bin/')

    kc = KafkaController(bindir, Path('./test_temp_can_delete'))
    print(f'port: {kc.port}')
    print(f'temp_dir: {kc.temp_dir}')
    kc.producer.send('mytopic', 'some message'.encode('utf-8'))

    # kc.clear_topic('mytopic')  # comment out to test consumer getting message
    # kc.clear_all_topics()  # comment out to test consumer getting message

    cons = KafkaConsumer(
        'mytopic',
        bootstrap_servers=[f'localhost:{kc.port}'],
        auto_offset_reset='earliest',
        group_id='foo')
    print(cons.poll(timeout_ms=1000))
    input('press enter to shut down')
    kc.destroy(True)


if __name__ == '__main__':
    main()
