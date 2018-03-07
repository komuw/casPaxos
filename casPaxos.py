# CASPaxos: Replicated State Machines without logs
# Denis Rystsov, rystsov.denis@gmail.com
# February 28, 2018
# https://arxiv.org/pdf/1802.07000.pdf


# Number of failures we can tolerate, F
# Number of nodes needed to tolerate F, failures is 2F+1
# eg to tolerate 2failures, we need 5 nodes
# specifically we are talking about 2F+1 acceptors.
# NB:: A node can be both an acceptor and a proposer.

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

    def generate_ballot_number(self, notLessThan=0):
        # we should never generate a random number that is equal to zero
        # since Acceptor.promise defaults to 0
        ballot_number = random.randint(notLessThan + 1, 100)
        return ballot_number

    def send_prepare(self):
        #  Generate ballot number, B and sends 'prepare' msg with that number to the acceptors.
        ballot_number = self.generate_ballot_number()
        confirmations_accepted_value = []
        for acceptor in self.acceptors:
            accepted_value, accepted_value_ballot_number = acceptor.prepare(
                ballot_number=ballot_number)
            confirmations_accepted_value.append(accepted_value)

        # Wait for the F + 1 confirmations
        while True:
            if len(confirmations_accepted_value) >= self.F + 1:
                break
            else:
                # sleep then check again
                time.sleep(5)
        
        # If they(confirmations) all contain the empty value,
        # then the proposer defines the current state as ∅ otherwise it picks the
        # value of the tuple with the highest ballot number.
        if sum(confirmations_accepted_value):
            # we are using 0 as ∅
            self.state = 0
        else:



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
            return "CONFLICT"
        self.promise = ballot_number
        return self.accepted
