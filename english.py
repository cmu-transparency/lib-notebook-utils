"""
Utilities related to the English language. Currently only contains
adjective/adverb/verb phrases indicating probability along with
experimentally derived perception of possibility.
"""

import utils.misc as misc

from typing import List

class Phrase(misc.LocalsMixin):  # pylint: disable=too-few-public-methods
    """ English phrase. """

    def __init__(self, text: str = None):
        self.text = text

    def __str__(self):
        return self.text
    #def __repr__(self): return f"Phrase({repr(self.text)})"

#class possibility(object):
#    """ Possibility and its statistics. """
#
#    def __init__(self, possibility: float, positive, normal

class Modifier(Phrase):  # pylint: disable=too-many-arguments,too-few-public-methods
    """ Modifier phrases (indicate possibility and positivity). """

    def __init__(self,
                 typ: str = "NounPhrase",
                 text: str = None,
                 possibility: float = None,
                 confidence: float = None):

        Phrase.__init__(self, text=text)

        self.possibility = possibility
        self.confidence = confidence
        self.typ = typ


     #def __str__(self):
        #return f"{self.text}/\t{self.possibility}@\t{self.confidence}"

    def __repr__(self):
        return "{self.typ}(\ttext={self.text},\
                  \tpossibility={self.possibility},\
                  \tconfidence={self.confidence}\
                 )".replace("\n", "")            # pylint: disable=E0001

class AdverbPhrase(Modifier):
    """ An adverb phrase. """

    def __init__(self,
                 text: str = None,
                 possibility: float = 0.5,
                 confidence: float = 0.5):

        Modifier.__init__(self, "AdverbPhrase", text,
                          possibility, confidence)

    def to_adverb(self):
        """ Convert to adverb phrase. """

        return self

class AdjectivePhrase(Modifier):
    """ An adjective phrase. """

    def __init__(self,
                 text: str = None,
                 possibility: float = 0.5,
                 confidence: float = 0.5):

        Modifier.__init__(self, "AdjectivePhrase", text,
                          possibility, confidence)

    def to_adverb(self):
        """ Convert to adverb phrase. """

        text = self.text
        ending = text[-1]
        if ending == "e":
            text = text[0:-1]+"ly"
        else:
            text = text+"ly"

        return AdverbPhrase(**self.locals(skip=["text", "typ"]),
                            text=text)

class NounPhrase(Modifier):
    """ A noun phrase. """

    def __init__(self,
                 text: str = None,
                 possibility: float = 0.5,
                 confidence: float = 0.5):

        Modifier.__init__(self, "NounPhrase", text,
                          possibility, confidence)

    def to_adverb(self):
        """ Convert to adverb phrase. """

        return AdverbPhrase(**self.locals(skip=["text", "typ"]),
                            text="with " + self.text)

class VerbPhrase(Modifier):
    """ A verb phrase. """

    def __init__(self,
                 text: str = None,
                 possibility: float = 0.5,
                 confidence: float = 0.5):

        Modifier.__init__(self, "VerbPhrase", text,
                          possibility, confidence)

    def to_adverb(self):
        """ Convert to adverb phrase. """

        return AdverbPhrase(**self.locals(skip=["text", "typ"]),
                            text=self.text + " to")

def _adv(prec: float, possibility: float, text: str):
    return AdverbPhrase(text=text,
                        possibility=2.0*(float(possibility)/100.-0.5),
                        confidence=(100.0-prec)/100.0)

def _adj(prec: float, possibility: float, text: str):
    return AdjectivePhrase(text=text,
                           possibility=2.0*(float(possibility)/100.-0.5),
                           confidence=(100.0-prec)/100.0)

def _nou(prec: float, possibility: float, text: str):
    return NounPhrase(text=text,
                      possibility=2.0*(float(possibility)/100.-0.5),
                      confidence=(100.0-prec)/100.0)

def _ver(prec: float, possibility: float, text: str):
    return VerbPhrase(text=text,
                      possibility=2.0*(float(possibility)/100.-0.5),
                      confidence=(100.0-prec)/100.0)

# phrases from Quantifying Probabilistic Expressions
# Author(s): Frederick Mosteller and Cleo Youtz
# Source: Statistical Science, Vol. 5, No. 1 (Feb., 1990), pp. 2-12
# https://pdfs.semanticscholar.org/a20c/7c3a5c41d4d7461074da38af95bfa1c78fd0.pdf
MY_PHRASES = [
    _adv(0.3, 99, "always"),
    _adj(1.1, 97, "certain"),
    _adv(5.5, 91, "almost always"),
    _nou(5.4, 91, "very high probability"),
    _adv(12.4, 87, "very often"),
    _adj(7.5, 86, "almost certain"),
    _adv(10.1, 85, "very likely"), # also adjective
    _adj(8.9, 85, "very probable"),
    _adj(14.5, 81, "very frequent"),
    _nou(10.1, 81, "high probability"),
    _nou(11.7, 81, "high chance"),
    _adv(16.7, 79, "usually"),     # poor confidence
    _adv(15.0, 69, "likely"),      # also adjective
    _adj(13.0, 69, "probable"),
    _adv(10.4, 69, "often"),
    _adj(17.9, 66, "liable to happen"),
    _adj(15.2, 61, "frequent"),
    _adv(3.3, 61, "more often than not"),
    _nou(6.9, 58, "better than even chance"),
    _nou(18.5, 52, "moderate probability"),
    _adv(0.6, 50, "as often as not"),
    _nou(0.5, 50, "even chance"),
    _adj(24.6, 45, "not infrequent"),
    _nou(5.4, 41, "less than even chance"),
    _adj(7.9, 38, "less often than not"),
    _adj(42.7, 37, "possible"),
    _nou(29.1, 37, "not unreasonable"),
    _ver(30.2, 36, "might happen"),
    _adv(17.5, 26, "sometimes"),        # poor confidence
    _adv(15.1, 23, "now and then"),     # poor confidence
    _adv(15.2, 22, "occasionally"),     # poor confidence
    _adv(16.3, 19, "unusually"),        # poor confidence
    _adj(12.5, 17, "infrequent"),
    _adv(12.5, 17, "once in a while"),
    _nou(14.5, 16, "low probability"),
    _adv(13.0, 16, "unlikely"),        # adjective ?
    _adj(14.7, 15, "improbable"),
    _adv(14.5, 14, "not often"),
    _adv(14.3, 13, "not very often"),
    _nou(11.3, 13, "poor chance"),
    _nou(7.8, 13, "low chance"),
    _adv(10.1, 12, "seldom"),          # also adjective
    _adv(5.9, 8, "very unlikely"),    # adjective ?
    _adj(6.4, 7, "very infrequent"),
    _adv(6.5, 7, "rarely"),
    _nou(5.7, 6, "very low probability"),
    _adj(5.9, 6, "very improbable"),
    _adv(4.5, 6, "very seldom"),
    _adv(3.8, 4, "very rarely"),
    _adv(3.4, 3, "almost never"),
    _adj(0.3, 1, "impossible"),
    _adv(0.3, 1, "never")
]

def closest_phrase(phrases, possibility):
    """ Get the closest phrase to the given possibility. """

    return sorted(phrases, key=lambda w: abs(possibility-w.possibility))[0]

def best_modifier_in_range(phrases, lower: float, upper: float):
    """ Get the best confidence modifier in the given range of possibility. """

    ok_phrases = list(filter(lambda w: w.possibility > lower and w.possibility < upper, phrases))
    best_phrases = sorted(ok_phrases, key=lambda w: w.confidence)
    if best_phrases:
        return best_phrases[-1]
    return None

def create_modifier_phrases(min_confidence=0.0, interval=0.1, types=lambda x: True):
    """ Create a list of best phrases to fill in each resolution interval. """

    phrases_good = [w for w in MY_PHRASES
                    if types(w) and w.confidence >= min_confidence]
    phrases_best = list(reversed(list(
        filter(
            lambda x: x is not None,
            [best_modifier_in_range(phrases_good,
                                    f-interval/2.0,
                                    f+interval/2.0)
             for f in misc.frange(-1.0 - interval,
                                  1.0 + interval,
                                  interval)]))))

    return phrases_best

def oxford_commas(al, fin: str):
    """
    Given a set of words, intersperse commas and fin before last
    word in the oxford commas style.
    """
    
    ll = len(al)
    if ll <= 1: return al
    if ll == 2: return [al[0], f" {fin} ", al[1]]  # pylint: disable=syntax-error
    ret = []
    ret.append(al.pop())
    for _ in range(len(al)-2):
        ret.append(", ")
        ret.append(al.pop())
    ret.append(", and ")
    ret.append(al.pop())

    return ret
