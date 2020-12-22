"""
    model.py

    Classes for modelling/abstracting Ghidra's JNI program/function/basic block/P-code/CFG.

    Because of requirements to be Python 2.7 for Ghidra, this script is compatible with both 2/3
"""
import collections
import json


class JsonClassSerializable(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, collections.Set):
            return dict(_set_object=list(obj))
        elif not hasattr(obj, '__dict__'):
            return dict(obj)
        else:
            return obj.__dict__

    def serialize(self):
        return json.dumps(self, cls=JsonClassSerializable)


# IMPORTANT: Corresponds to 'from ghidra.program.model.pcode.PcodeOp import PCODE_MAX'
PCODE_MAX = 70


class Code_PCode(JsonClassSerializable):
    def __init__(self, mnemonic='', opcode=-1):
        ''' Represents Ghidra's P-code associated with PcodeBasicBlock

        Keyword arguments:
        mnemonic: str -- Text label for P-code operand
        opcode: int -- Integer for P-code opcode
        '''
        self.mnemonic = mnemonic
        self.opcode = opcode

    @staticmethod
    def deserialize(json_string):
        return Code_PCode(**json.loads(json_string))


class Code_Basic_Block(JsonClassSerializable):
    def __init__(self, start_address=0, end_address=0, opcodes=[], incoming_bbs=[], outgoing_bbs=[]):
        ''' Represents Ghidra's PcodeBasicBlock

        Keyword arguments:
        start_address: int -- Starting address of BB
        end_address: int -- End address of BB
        opcodes: list(Code_PCode) -- Ordered list of P-code instructions
        incoming_bbs: list(BB.start_address) -- list of basic blocks (by start_address) calling this basic block
        outgoing_bbs: list(BB.start_address) -- list of basic blocks (by start_address) called from this basic block
        '''
        self.start_address = start_address
        self.end_address = end_address
        self.incoming_bbs = incoming_bbs
        self.outgoing_bbs = outgoing_bbs
        if isinstance(opcodes, list):
            self.opcodes = [Code_PCode(**x) for x in opcodes]
        else:
            raise TypeError('opcodes must be provided as a list of Code_PCode objects')

    @staticmethod
    def deserialize(json_string):
        return Code_Basic_Block(**json.loads(json_string))


class Code_Function(JsonClassSerializable):
    def __init__(self, start_address=0, end_address=0, name='', basic_blocks={}):
        ''' Represents Ghidra's Function object

        Keyword arguments:
        start_address: int -- Starting address of function
        end_address: int -- End address of function
        name: str -- Symbolic name of function
        basic_blocks: dict(BB.start_address, Code_Basic_Block) -- dict of basic blocks contained within this function
        '''
        self.start_address = start_address
        self.end_address = end_address
        self.name = name
        if isinstance(basic_blocks, dict):
            self.basic_blocks = {addr: Code_Basic_Block(**bb) for addr, bb in basic_blocks.items()}
        else:
            raise TypeError('basic_blocks must be provided as a dictionary of str: Code_Basic_Block objects')

    @staticmethod
    def deserialize(json_string):
        return Code_Function(**json.loads(json_string))


class Code_File(JsonClassSerializable):
    def __init__(self, name='', functions=[]):
        ''' Represents Ghidra's Function object

        Keyword arguments:
        name: str -- Symbolic name of file/program
        functions: list(Code_Function) -- list of functions contained within this program
        '''
        self.name = name
        if isinstance(functions, list):
            self.functions = [Code_Function(**x) for x in functions]
        else:
            raise TypeError('functions must be provided as a list of Code_Function objects')

    @staticmethod
    def deserialize(json_string):
        return Code_File(**json.loads(json_string))
