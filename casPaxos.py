# CASPaxos: Replicated State Machines without logs
# Denis Rystsov, rystsov.denis@gmail.com
# February 28, 2018
# https://arxiv.org/pdf/1802.07000.pdf

# More docs:
# 1. https://www.youtube.com/watch?v=SRsK-ZXTeZ0 # Paxos Simplified
# 2. https://www.youtube.com/watch?v=d7nAGI_NZPk # Google tech talks, The Paxos Algorithm


# Number of failures we can tolerate, F
# Number of nodes needed to tolerate F, failures is 2F+1
# eg to tolerate 2failures, we need 5 nodes
# specifically we are talking about 2F+1 acceptors.
# NB:: A node can be both an acceptor and a proposer.

import time
import random


class Client(object):
    """
    1. A client submits the f change function to a proposer.
    """
    pass


class Proposer(object):
    """
    2. The proposer generates a ballot number, B, and sends ”prepare” messages containing that number
    to the acceptors.

    5. Waits for the F + 1 confirmations
    6. If they all contain the empty value, then the proposer defines the current state as ∅ otherwise it picks the value of the tuple with the highest ballot number.
    7. Applies the f function to the current state and sends the result, new state, along with the generated ballot number B (an ”accept” message) to the acceptors.

    10. Waits for the F + 1 confirmations.
    11. Returns the new state to the client.
    """

    def __init__(self, acceptors):
        self.acceptors = acceptors
        # since we need to have 2F+1 acceptors to tolerate F failures, then:
        self.F = (self.acceptors - 1) / 2
        self.state = 0

    def receive(self, value, f):
        """
        receives request from client and begins consensus process.
        """
        #  Generate ballot number, B and sends 'prepare' msg with that number to the acceptors.
        ballot_number = self.generate_ballot_number()
        self.send_prepare(ballot_number=ballot_number)
        result = self.send_accept(f, ballot_number)

    def generate_ballot_number(self, notLessThan=0):
        # we should never generate a random number that is equal to zero
        # since Acceptor.promise defaults to 0
        ballot_number = random.randint(notLessThan + 1, 100)
        return ballot_number

    def send_prepare(self, ballot_number):
        confirmations = []  # list of tuples conatining accepted (value, ballotNumOfAcceptedValue)
        for acceptor in self.acceptors:
            confirmation = acceptor.prepare(ballot_number=ballot_number)
            if confirmation[0] == "CONFLICT":
                # CONFLICT, do something
                pass
            else:
                confirmations.append(confirmation)

        # Wait for the F + 1 confirmations
        while True:
            if len(confirmations) >= self.F + 1:
                break
            else:
                # sleep then check again
                time.sleep(5)

        total_list_of_confirmation_values = []
        for i in confirmations:
            total_list_of_confirmation_values.append(confirmations[0])

        # If they(confirmations) all contain the empty value,
        # then the proposer defines the current state as ∅ otherwise it picks the
        # value of the tuple with the highest ballot number.
        if sum(total_list_of_confirmation_values) == 0:
            # we are using 0 as ∅
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
        7. Applies the f function to the current state and sends the result, new state, along with the generated ballot number B (an ”accept” message) to the acceptors.
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

        # Wait for the F + 1 confirmations
        while True:
            if len(acceptations) >= self.F + 1:
                break
            else:
                # sleep then check again
                time.sleep(5)

        # Returns the new state to the client.
        return self.state


class Acceptor(object):
    """
    3. Returns a conflict if it already saw a greater ballot number.
    else
    4. Persists the ballot number as a promise and returns a confirmation either;
    with an empty value (if it hasn’t accepted any value yet)
    or
    with a tuple of an accepted value and its ballot number.

    8. Returns a conflict if it already saw a greater ballot number.
    9. Erases the promise, marks the received tuple (ballot number, value) as the accepted value and returns a confirmation
    """
    promise = 0  # ballot number
    accepted = (0, 0)

    def prepare(self, ballot_number):
        """
        3. Returns a conflict if it already saw a greater ballot number.
        else
        4. Persists the ballot number as a promise and returns a confirmation either;
        with an empty value (if it hasn’t accepted any value yet)
        or
        with a tuple of an accepted value and its ballot number.
        """
        if self.promise > ballot_number:
            return ("CONFLICT", "CONFLICT")
        self.promise = ballot_number
        return self.accepted

    def accept(self, ballot_number, new_state):
        """
        8. Returns a conflict if it already saw a greater ballot number.
        9. Erases the promise, marks the received tuple (ballot number, value) as the accepted value and returns a confirmation
        """
        if self.promise > ballot_number:
            return ("CONFLICT", "CONFLICT")
        self.promise = 0
        self.accepted = (new_state, ballot_number)
        return ("CONFIRM", "CONFIRM")
