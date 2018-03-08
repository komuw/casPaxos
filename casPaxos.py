# CASPaxos: Replicated State Machines without logs
# Denis Rystsov, rystsov.denis@gmail.com
# February 28, 2018
# https://arxiv.org/pdf/1802.07000.pdf

# More docs:
# 1. https://www.youtube.com/watch?v=SRsK-ZXTeZ0 # Paxos Simplified
# 2. https://www.youtube.com/watch?v=d7nAGI_NZPk # Google tech talks, The Paxos Algorithm
# 3. http://rystsov.info/2015/09/16/how-paxos-works.html
# 4. https://github.com/peterbourgon/caspaxos


# Number of failures we can tolerate, F
# Number of nodes needed to tolerate F, failures is 2F+1
# eg to tolerate 2failures, we need 5 nodes
# specifically we are talking about 2F+1 acceptors.
# NB:: A node can be both an acceptor and a proposer.

import time
import random
import logging


logger = logging.getLogger()
handler = logging.StreamHandler()
formatter = logging.Formatter('%(message)s')
handler.setFormatter(formatter)
logger.addHandler(handler)
logger.setLevel('DEBUG')


class Client(object):
    """
    1. A client submits the f change function to a proposer.
    """
    pass


class Proposer(object):
    """
    2. The proposer generates a ballot number, B, and sends "prepare" messages containing that number
    to the acceptors.

    5. Waits for the F + 1 confirmations
    6. If they all contain the empty value, then the proposer defines the current state as PHI otherwise it picks the value of the tuple with the highest ballot number.
    7. Applies the f function to the current state and sends the result, new state, along with the generated ballot number B (an "accept" message) to the acceptors.

    10. Waits for the F + 1 confirmations.
    11. Returns the new state to the client.
    """

    def __init__(self, acceptors):
        # TODO: note that a node can be both a proposer and an acceptor at the same time
        # in fact most times they usually are.
        # So we should add logic to handle that fact.
        if not isinstance(acceptors, list):
            raise ValueError("acceptors ought to be a list of child classes of Acceptor object")
        self.acceptors = acceptors
        # since we need to have 2F+1 acceptors to tolerate F failures, then:
        self.F = (len(self.acceptors) - 1) / 2
        self.state = 0
        self.current_highest_ballot = 1
        logger.info(
            "Init Proposer. acceptors={0}. F={1}. initial_state={2}. current_highest_ballot={3}".format(
                self.acceptors,
                self.F,
                self.state,
                self.current_highest_ballot))

    def receive(self, f):
        """
        receives f change function from client and begins consensus process.
        """
        #  Generate ballot number, B and sends 'prepare' msg with that number to the acceptors.
        ballot_number = self.generate_ballot_number(notLessThan=self.current_highest_ballot)
        logger.info("\nreceive. change_func={0}. ballot_number={1}.".format(f, ballot_number))
        self.send_prepare(ballot_number=ballot_number)
        result = self.send_accept(f, ballot_number)

        self.current_highest_ballot = ballot_number
        return result

    def generate_ballot_number(self, notLessThan=0):
        """
        http://rystsov.info/2015/09/16/how-paxos-works.html
        Each server may have unique ID and use an increasing sequence of natural number n to generate (n,ID) tuples and use tuples as ballot numbers.
        To compare them we start by comparing the first element from each tuple. If they are equal, we use the second component of the tuple (ID) as a tie breaker.
        Let IDs of two servers are 0 and 1 then two sequences they generate are (0,0),(1,0),(2,0),(3,0).. and (0,1),(1,1),(2,1),(3,1).. Obviously they are unique, ordered and for any element in one we always can peak an greater element from another.
        """
        # we should never generate a random number that is equal to zero
        # since Acceptor.promise defaults to 0
        ballot_number = random.randint(notLessThan + 1, notLessThan + 3)
        return ballot_number

    def send_prepare(self, ballot_number):
        confirmations = []  # list of tuples conatining accepted (value, ballotNumOfAcceptedValue)
        for acceptor in self.acceptors:
            confirmation = acceptor.prepare(ballot_number=ballot_number)
            if confirmation[0] == "CONFLICT":
                # CONFLICT, do something
                # We should fast-forward our ballot number's counter to
                # the highest number we saw from the conflicted preparers, so a
                # subsequent proposal might succeed. We could try to re-submit the same
                # request with our updated ballot number, but for now let's leave that
                # responsibility to the caller.
                # borrowed from:
                # https://github.com/peterbourgon/caspaxos/blob/4374c3a816d7abd6a975e0e644782f0d03a2d05d/protocol/local_proposer.go#L148-L154
                pass
            else:
                confirmations.append(confirmation)

        # Wait for the F + 1 confirmations
        if len(confirmations) < self.F + 1:
            raise ValueError(
                "Unable to get enough confirmations. got={0}. wanted={1}".format(
                    len(confirmations), self.F + 1))

        total_list_of_confirmation_values = []
        for i in confirmations:
            total_list_of_confirmation_values.append(i[0])

        # If they(confirmations) all contain the empty value,
        # then the proposer defines the current state as PHI otherwise it picks the
        # value of the tuple with the highest ballot number.
        if sum(total_list_of_confirmation_values) == 0:
            # we are using 0 as PHI
            self.state = 0
        else:
            highest_confirmation = self.get_highest_confirmation(confirmations)
            self.state = highest_confirmation[0]

    def get_highest_confirmation(self, confirmations):
        ballots = []
        for i in confirmations:
            ballots.append(i[1])
        ballots = sorted(ballots)
        highestBallot = ballots[len(ballots) - 1]

        for i in confirmations:
            if i[1] == highestBallot:
                return i

    def send_accept(self, f, ballot_number):
        """
        7. Applies the f function to the current state and sends the result, new state, along with the generated ballot number B (an "accept" message) to the acceptors.
        """
        self.state = f(self.state)
        acceptations = []
        for acceptor in self.acceptors:
            acceptation = acceptor.accept(ballot_number=ballot_number, new_state=self.state)
            if acceptation[0] == "CONFLICT":
                # CONFLICT, do something
                pass
            else:
                acceptations.append(acceptation)

        # Wait for the F + 1 acceptations
        if len(acceptations) < self.F + 1:
            raise ValueError(
                "Unable to get enough acceptations. got={0}. wanted={1}".format(
                    len(acceptations), self.F + 1))
        # Returns the new state to the client.
        return self.state


class Acceptor(object):
    """
    3. Returns a conflict if it already saw a greater ballot number.
    else
    4. Persists the ballot number as a promise and returns a confirmation either;
    with an empty value (if it hasn't accepted any value yet)
    or
    with a tuple of an accepted value and its ballot number.

    8. Returns a conflict if it already saw a greater ballot number.
    9. Erases the promise, marks the received tuple (ballot number, value) as the accepted value and returns a confirmation
    """
    promise = 0  # ballot number
    accepted = (0, 0)

    def __init__(self, name):
        self.name = name

    def prepare(self, ballot_number):
        """
        3. Returns a conflict if it already saw a greater ballot number.
        else
        4. Persists the ballot number as a promise and returns a confirmation either;
        with an empty value (if it hasn't accepted any value yet)
        or
        with a tuple of an accepted value and its ballot number.
        """
        logger.info("prepare. name={0}. ballot_number={1}. promise={2}. accepted={3}".format(
            self.name, ballot_number, self.promise, self.accepted))
        if self.promise > ballot_number:
            return ("CONFLICT", "CONFLICT")
        self.promise = ballot_number
        return self.accepted

    def accept(self, ballot_number, new_state):
        """
        8. Returns a conflict if it already saw a greater ballot number.
        9. Erases the promise, marks the received tuple (ballot number, value) as the accepted value and returns a confirmation
        """
        # # induce failure
        # if self.name in ['a1', 'a2', 'a3']:
        #     self.promise = 1000
        # if self.name in ['a1']:
        #     import pdb;pdb.set_trace()
        logger.info("accept. name={0}. ballot_number={1}. new_state={2}. promise={3}. accepted={4}".format(
            self.name, ballot_number, new_state, self.promise, self.accepted))
        if self.promise > ballot_number:
            return ("CONFLICT", "CONFLICT")
        elif self.accepted[1] > ballot_number:
            # https://github.com/peterbourgon/caspaxos/blob/4374c3a816d7abd6a975e0e644782f0d03a2d05d/protocol/memory_acceptor.go#L118-L128
            return ("CONFLICT", "CONFLICT")

        self.promise = 0
        self.accepted = (new_state, ballot_number)
        return ("CONFIRM", "CONFIRM")


a1 = Acceptor(name='a1')
a2 = Acceptor(name='a2')
a3 = Acceptor(name='a3')
a4 = Acceptor(name='a4')
a5 = Acceptor(name='a5')


def change_func(state):
    """
    http://rystsov.info/2015/09/16/how-paxos-works.html
    It's a common practice for storages to have write operations to mutate its state and read operations to query it.
    Paxos is different, it guarantees consistency only for write operations,
    so to query/read its state the system makes read, writes the state back and when the state change is accepted the system queries the written state.

    ie:
    It's impossible to read a value in Single Decree Paxos without modifying the state of the system.
    For example if you connect only to one acceptor then you may get stale read.
    If you connect to a quorum of the acceptors then each acceptor may return a different value, so you should pick a value with the largest ballot number and send it back to acceptors.
    Once you get a confirmation of the successful write then you can return the written value to the client as the current value.
    So in order to read you should write.
    """
    return state + 3


def read_func(state):
    return state


def set_func(state):
    return state + 3


acceptorsList = [a1, a2, a3, a4, a5]
p = Proposer(acceptors=acceptorsList)

print "\n#############################"
for i in range(1, 100):
    if (i % 2) == 0:
        result = p.receive(read_func)
    else:
        result = p.receive(set_func)
    print "result2", result
