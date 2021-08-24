# type: ignore

### Standard imports. ###

import pprint


class Meta(dict):

    """ Carries information about an observation across the pipeline. """

    # Required attributes and their types. If they are not provided, they will
    # be set to None.

    _required_attrs_ = {
        "fname": str,
        "proc_dm_trials": int,
        "num_candidates": int,
        "num_fold_prfs": int,
        "num_arv_prfs": int,
    }

    def __init__(self, attrs):

        """Create new Meta from a filterbank file.

        The 'attrs' dictionary is expected to contain a certain number of required
        keys with a specific type, at the moment these are:

            fname:          Name of the file.
            proc_dm_trials: Number of DM trials processed.
            num_candidates: Number of candidates generated.
            num_fold_prfs:  Number of folded profiles made.
            num_arv_prfs:   Number of folded profiles archived.
        """

        super(Meta, self).__init__(attrs)
        self._finalise()

    def __str__(self):
        return "Meta\n%s" % pprint.pformat(dict(self))

    def __repr__(self):
        return str(self)

    def _finalise(self):

        """Check that required keys are either absent, in which case they will
        be set to None, or present with the correct type.
        """

        req = self._required_attrs_
        for key in req.keys():
            val = self.get(key, None)
            if val is None:
                self[key] = val
            elif not isinstance(val, req[key]):
                msg = "Meta key '{k:s}' must have type '{t:s}' instead of '{ti:s}'".format(
                    k=key, t=req[key].__name__, ti=type(val).__name__
                )
                raise ValueError(msg)
