# this is network manager!

import logging
from threading import Timer

from coap import coap

from openvisualizer.moteState import moteState

log = logging.getLogger('networkManager')
log.setLevel(logging.ERROR)
log.addHandler(logging.NullHandler())

from openvisualizer.eventBus import eventBusClient


class NetworkManager(eventBusClient.eventBusClient):

    def __init__(self):
        # log
        log.info("Network Manager started!")

        # store params

        # initialize parent class
        eventBusClient.eventBusClient.__init__(
            self,
            name='NetworkManager',
            registrations=[
                {
                    'sender': self.WILDCARD,
                    'signal': 'networkChanged',
                    'callback': self._networkChanged_notif,
                },
                {
                    'sender': self.WILDCARD,
                    'signal': 'updateRootMoteState',
                    'callback': self._updateRootMoteState_notif,
                }
            ]
        )

        # local variables
        self.max_assignable_slot = 5
        self.start_offset = 4
        self.max_assignable_channel = 16
        self.lastNetworkUpdateCounter = 0
        self.max_entry_per_packet = 10
        self.motes = None
        self.edges = None
        self.scheduleTable = []
        self.dag_root_moteState = None


    # ======================== public ==========================================
    def close(self):
        pass

    def getSchedule(self):
        return self.scheduleTable

    # ======================== private =========================================
    def _networkChanged_notif(self,sender,signal,data):
        log.info("Get network changed")
        self.lastNetworkUpdateCounter += 1
        log.debug("New counter: {0}".format(self.lastNetworkUpdateCounter))
        self.motes = data[0]
        self.edges = data[1]
        # wait x second for newer dao
        timer = Timer(15, self._doCalculate, [self.lastNetworkUpdateCounter])
        timer.start()
        log.debug("End!")

    def _doCalculate(self, *args, **kwargs):
        if self.lastNetworkUpdateCounter > args[0]:
            log.debug("[PASS] Calculate counter: {0} is older than {1}".format(args[0], self.lastNetworkUpdateCounter))
            return
        log.debug("Real calculate! {0}".format(args[0]))
        motes = self.motes
        edges = self.edges
        log.debug("Mote count: {0}".format(len(motes)))
        log.debug("Edge count: {0}".format(len(edges)))
        log.debug("Start algorithm")
        results = self._simplestAlgorithms(motes, edges, self.max_assignable_slot, self.start_offset, self.max_assignable_channel)
        log.debug("End algorithm")
        log.debug("| From |  To  | Slot | Chan |")
        for item in results:
            log.debug("| {0:4} | {1:4} | {2:4} | {3:4} |".format(item[0][-4:], item[1][-4:], item[2], item[3]))
        log.debug("==============================")
        self.scheduleTable = results
        self._sendScheduleTableToMote(motes)

    def _sendScheduleTableToMote(self, motes):
        log.debug("Starting sending schedule. Total entry: {0}".format(len(self.scheduleTable)))
        for mote in motes:
            log.debug("Parsing {0}".format(mote))
            entryCount = 0
            is_root = False
            if mote[-2:] == '88':   # TODO make it better
                is_root = True
            entrys = list()

            for schedule in self.scheduleTable:
                entry_type = -1
                neighbor = ''
                if schedule[0] == mote:
                    entry_type = 0x40           # TX
                    neighbor = schedule[1]
                elif schedule[1] == mote:
                    entry_type = 0x00           # RX
                    neighbor = schedule[0]

                if entry_type != -1:
                    schedule_entry = list()
                    schedule_entry.append(schedule[2])  # slot offset
                    schedule_entry.append(schedule[3])  # channel offset
                    schedule_entry.append(entry_type)         # type
                    # address
                    for byte in neighbor.split(':'):
                        schedule_entry.append(int(byte[:2], 16))
                        schedule_entry.append(int(byte[-2:], 16))
                    entrys.append(schedule_entry)

            log.debug("{0} have {1} entry to send. [max entry per packet:{2}]".format(mote, len(entrys), self.max_entry_per_packet))

            payload = list()
            for index in xrange(0, len(entrys), self.max_entry_per_packet):
                packet_sequence = index / self.max_entry_per_packet
                entry_group = entrys[index: index + self.max_entry_per_packet]
                log.debug("Sequence {0} have {1} entry".format(packet_sequence, len(entry_group)))

                # if index % self.max_entry_per_packet == 0:
                payload = list()
                if index == 0:
                    payload.append(0x80)    # first
                else:
                    payload.append(0x00)    # not first

                payload.append(len(entry_group))
                log.debug("Group Entry length: {0}".format(len(entry_group)))

                for entry in entry_group:
                    payload.extend(entry)

                payload = bytearray(payload)
                self._sendPayloadToMote(mote, payload, is_root)

            log.debug("{0} done".format(mote))

        log.debug("All Done!!#########################################")

    def _sendPayloadToMote(self, mote_address, payload, is_root):
        if is_root:
            log.debug("GO root")
            self.dispatch(
                signal='cmdToMote',
                data={
                    'serialPort': self.dag_root_moteState.moteConnector.serialport,
                    'action': self.dag_root_moteState.ADD_SCHEDULE,
                    'payload': payload
                },
            )
            return
        else:
            try:

                log.debug("GO mote")
                c = coap.coap(udpPort=5466)
                c.maxRetransmit = 2
                p = c.POST('coap://[bbbb::{0}]/green'.format(mote_address), payload=payload)
                c.close()
            except:
                log.error("Got Error!")

            log.debug("====================================")
        return

    def _updateRootMoteState_notif(self, sender, signal, data):
        log.debug("Get update root")
        log.debug(data)
        self.dag_root_moteState = data['rootMoteState']
        return

    def _simplestAlgorithms(self, motes, edges, max_assignable_slot, start_offset, max_assignable_channel):
        results = []
        in_motes = {}
        for mote in motes:
            in_motes[mote] = dict()
        for edge in edges:
            fromMote, fromMoteKey = in_motes[edge['u']], edge['u']
            toMote, toMoteKey = in_motes[edge['v']], edge['v']
            assigned = False
            for slotOffset in range(start_offset, start_offset + max_assignable_slot):
                log.debug("Trying: {0}".format(slotOffset))
                if slotOffset not in fromMote and slotOffset not in toMote:
                    log.debug("assign {0} -> {1}, using: {2}".format(fromMoteKey, toMoteKey, slotOffset))

                    fromMote[slotOffset] = [1, toMoteKey]
                    toMote[slotOffset] = [0, fromMoteKey]
                    results.append([fromMoteKey, toMoteKey, slotOffset, 0])
                    assigned = True
                    break
            if assigned == False:
                log.error("Cannot assign! {0}".format(edge))

        log.info("Done calculate!")
        return results
