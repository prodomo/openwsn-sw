# Copyright (c) 2010-2013, Regents of the University of California. 
# All rights reserved. 
#  
# Released under the BSD 3-Clause license as published at the link below.
# https://openwsn.atlassian.net/wiki/display/OW/License
'''
Module which receives DAO messages and calculates source routes.

.. moduleauthor:: Xavi Vilajosana <xvilajosana@eecs.berkeley.edu>
                  January 2013
.. moduleauthor:: Thomas Watteyne <watteyne@eecs.berkeley.edu>
                  April 2013
'''
import logging
log = logging.getLogger('topology')
log.setLevel(logging.ERROR)
log.addHandler(logging.NullHandler())

import threading
import time
from datetime import timedelta

import openvisualizer.openvisualizer_utils as u
from openvisualizer.eventBus import eventBusClient

class topology(eventBusClient.eventBusClient):

    def __init__(self):

        # local variables
        self.dataLock        = threading.Lock()
        self.parents         = {}
        self.parentsLastSeen = {}
        self.NODE_TIMEOUT_THRESHOLD = 300


        eventBusClient.eventBusClient.__init__(
            self,
            name                  = 'topology',
            registrations         =  [
                {
                    'sender'      : self.WILDCARD,
                    'signal'      : 'updateParents',
                    'callback'    : self.updateParents,
                },
                {
                    'sender'      : self.WILDCARD,
                    'signal'      : 'getParents',
                    'callback'    : self.getParents,
                },
            ]
        )

    #======================== public ==========================================

    def getParents(self,sender,signal,data):
        return self.parents

    def getDAG(self):
        states = []
        edges = []
        motes = []

        with self.dataLock:
            for src, dsts in self.parents.iteritems():
                src_s = ''.join(['%02X' % x for x in src[-2:] ])
                motes.append(src_s)
                for dst in dsts:
                    dst_s = ''.join(['%02X' % x for x in dst[-2:] ])
                    edges.append({ 'u':src_s, 'v':dst_s })
                    motes.append(dst_s)
            motes = list(set(motes))
            for mote in motes:
                d = { 'id': mote, 'value': { 'label': mote } }
                states.append(d)

        return states, edges

    def getDAGForNetworkManager(self):
        motes = []
        edges = []

        with self.dataLock:
            for src, dsts in self.parents.iteritems():
                it = iter(src)
                src_s = ':'.join(["{0:02x}{1:02x}".format(x, next(it)) for x in it])
                motes.append(src_s)
                for dst in dsts:
                    it = iter(dst)
                    dst_s = ':'.join(["{0:02x}{1:02x}".format(x, next(it)) for x in it])
                    edges.append({'u': src_s, 'v': dst_s})
                    motes.append(dst_s)
        motes = list(set(motes))

        return motes, edges


    def updateParents(self,sender,signal,data):
        ''' inserts parent information into the parents dictionary '''
        log.debug("Event: updateParents")
        change_found = False
        with self.dataLock:
            #data[0] == source address, data[1] == list of parents
            if data[0] in self.parents and self.parents[data[0]] == data[1]:
                # nothing
                change_found = False
            else:
                change_found = True
                self.parents.update({data[0]:data[1]})
            self.parentsLastSeen.update({data[0]: time.time()})
        timeout_found = self._clearNodeTimeout()

        if not change_found and not timeout_found:
            log.debug("No change!")
        else:
            log.debug("Yes change!")
            motes, edges = self.getDAGForNetworkManager()
            self.dispatch(
                signal='networkChanged',
                data=(motes, edges)
            )


    def _clearNodeTimeout(self):
        threshold = time.time() - self.NODE_TIMEOUT_THRESHOLD
        timeout_found = False
        with self.dataLock:
            for node in self.parentsLastSeen.keys():
                if self.parentsLastSeen[node] < threshold:
                    timeout_found = True
                    if node in self.parents:
                        del self.parents[node]
                    del self.parentsLastSeen[node]
        return timeout_found

    def recalculate(self):
        motes, edges = self.getDAGForNetworkManager()
        self.dispatch(
            signal='networkChanged',
            data=(motes, edges)
        )

    #======================== private =========================================

    #======================== helpers =========================================
