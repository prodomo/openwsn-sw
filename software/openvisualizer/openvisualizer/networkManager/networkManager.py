# this is network manager!

import logging
from threading import Timer

from coap import coap

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
            ]
        )

        # local variables
        self.max_assignable_slot = 5
        self.start_offset = 4
        self.max_assignable_channel = 16
        self.lastNetworkUpdateCounter = 0
        self.motes = None
        self.edges = None
        self.scheduleTable = []


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
        self._sendScheduleToMote(motes)

    def _sendScheduleToMote(self, motes):
        log.debug("Starting sending schedule. Total entry: {0}".format(len(self.scheduleTable)))
        for mote in motes:
            log.debug("Parsing {0}".format(mote))
            entryCount = 0
            isRoot = False
            if mote[-2:] == '01':
                isRoot = True

            payload = list()
            payload.append(0x11)        # reserve
            payload.append(0x00)        # entry count. later will change it's value

            for schedule in self.scheduleTable:
                type = -1
                neighbor = ''
                if schedule[0] == mote:
                    type = 1
                    neighbor = schedule[1]
                elif schedule[1] == mote:
                    type = 0
                    neighbor = schedule[0]

                if type != -1:
                    entryCount += 1
                    payload.append(schedule[2])  # slot offset
                    payload.append(schedule[3])  # channel offset
                    payload.append(type)         # type
                    # address
                    for byte in neighbor.split(':'):
                        payload.append(int(byte[:2], 16))
                        payload.append(int(byte[-2:], 16))

            # set entry count
            payload[1] = entryCount
            payload = bytearray(payload)
            if isRoot:
                log.debug("GO root")
                import socket
                sock6 = socket.socket(socket.AF_INET6, socket.SOCK_DGRAM)
                sock6.sendto(payload, ('bbbb::{0}'.format(mote), 5683))
                # c = coap.coap()
                # p = c.POST('coap://[bbbb::1415:92cc:0:{0}]/green'.format(moteKey), payload = payload,confirmable=False)
                # c.close()
                # log.debug("Root dwon")
                #ms.triggerAction(moteState.moteState.COMMAND_SET_ADD_SCHEDULE)
                # self.dispatch(
                #     signal          = 'cmdToMote',
                #     data            =  "ffff"
                #
            else:
                log.debug("GO mote")
                c = coap.coap()
                p = c.POST('coap://[bbbb::{0}]/green'.format(mote), payload = payload)
                c.close()
            log.debug("====================================")
        log.debug("All Done!!#########################################")


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
