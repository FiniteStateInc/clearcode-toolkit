"""
    lsh_hash.py

    Algorithm for performing function hashing over IR representations
"""
import hashlib
import numpy as np

from . import model

# type aliases
Vector = np.ndarray


# ### FUNCTION HASHING FUNCTIONS #####
def gen_planes(num_planes: int = 100) -> Vector:
    """
    Generates a series of arbitrary hyper planes to be used for "bucketing" in our LSH.

    :param num_planes: the number of hyper planes to generate
    :return: a numpy array containing num_planes pythonic arrays
    """
    # TODO: look into alternate (deterministic) methods of generating uniformly distributed hyperplanes
    h_planes = np.array([[(-1 ^ int((i * j) / 2)) * (i ^ j) % (int(i / (j + 1)) + 1) for j in range(model.PCODE_MAX)]
                         for i in range(num_planes)])

    return h_planes


def vectorize(bb):
    """
    Converts a basic block into a vector representing the number of occurrences of each "type" of IL instruction,
    effectively forming a "bag of words".

    :param bb: the PcodeBasicBlock to construct a vector representation of
    :return: a numpy array representing the number of occurrences of each IL instruction type
    """
    vector = dict((key, 0) for key in range(model.PCODE_MAX))
    for op in bb.opcodes:
        vector[op.opcode] += 1
    return np.fromiter(vector.values(), dtype=int)


def bucket(vector, h_planes):
    """
    Encode a vector's position relative to each hyper plane such that similar vectors will land in the same "bucket".

    :param vector: the vector to encode the position of
    :param h_planes: a numpy array of hyperplanes
    :return: a hex string representing the "bucket" the given vector lands in
    """
    bools = []
    for i in range(len(h_planes)):
        bools.append(str(int(np.dot(vector, h_planes[i]) > 0)))
    return hex(int(''.join(bools), 2))


def weisfeiler_lehman(bbs, iterations=1):
    """
    Hash each function using a variation of the Weisfeiler-Lehman kernel.
    This allows us to account for not only the contents of each basic block, but also the overall "structure" of the CFG

    See https://blog.quarkslab.com/weisfeiler-lehman-graph-kernel-for-binary-function-analysis.html for more info.

    :param bbs: the dictionary mapping basic blocks in the function to their calculated "bucket"
    :param iterations: the number of levels of "neighbors" to account for in the Weisfeiler-Lehman kernel
    :return: a string representing the function's hash
    """
    old_labels = bbs

    # TODO: experiment with different # iterations to find a reasonable default
    new_labels = {}
    for _ in range(iterations):
        for bb in bbs.keys():
            new_labels[bb] = ''

            for src_bb_addr in bb.incoming_bbs:
                src_bb = next((x for x in bbs.keys() if x.start_address == src_bb_addr), None)
                new_labels[bb] += '{}'.format(old_labels[src_bb])

            for dest_bb_addr in bb.outgoing_bbs:
                dest_bb = next((x for x in bbs.keys() if x.start_address == dest_bb_addr), None)
                new_labels[bb] += '{}'.format(old_labels[dest_bb])

        old_labels = new_labels

    # The set of labels associated with each basic block now captures the relationship between basic blocks in the CFG,
    # however, a function with many CFGs will have a very long set of labels. Hash this list again for hash consistency
    long_hash = ''.join(sorted(new_labels.values()))
    m = hashlib.sha256()
    m.update(long_hash.encode('utf-8'))
    return m.hexdigest()


# global planes here, only need to be calculated once
H_PLANES = gen_planes()

# NULL_HASH, calculated once, used to filter out any functions that ~ roughly equal an empty function
NULL_HASH = hashlib.sha256()
NULL_HASH.update(''.encode('utf-8'))
NULL_HASH = NULL_HASH.hexdigest()


def hash_function(func: model.Code_Function) -> str:
    bb_hashes = {}
    for bb in func.basic_blocks.values():
        bb_hashes[bb] = bucket(vectorize(bb), H_PLANES)

    # Need more than zero BBs for any possible variation
    if len(bb_hashes) > 0:
        return weisfeiler_lehman(bb_hashes)
    else:
        return NULL_HASH
