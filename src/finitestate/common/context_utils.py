from contextlib import AbstractContextManager


class safe_closing(AbstractContextManager):
    """Context to automatically close something at the end of a block.  This is a variant of the built-in closing()
    method from contextlib, but one which will safely ignore a None argument passed to the context.  If the argument
    is non-None, it must, of course, have a close() method to be called.

    Code like this:

        with closing(<module>.open(<arguments>)) as f:
            <block>

    is equivalent to this:

        f = <module>.open(<arguments>)
        try:
            <block>
        finally:
            f.close()

    """
    def __init__(self, thing):
        self.thing = thing

    def __enter__(self):
        return self.thing

    def __exit__(self, *exc_info):
        if self.thing is not None:
            self.thing.close()
