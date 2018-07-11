import utils.misc as misc

class Phrase(misc.withlocals):
    def __init__(self, text=None):
        self.text = text
        
    def __str__(self): return self.text
    #def __repr__(self): return f"Phrase({repr(self.text)})"
        
class Modifier(Phrase):
    def __init__(self, typ="NounPhrase", text=None, positive=True, strength=0.5, normal=False, precision=0.5):
        Phrase.__init__(self, text=text)
        self.positive = positive
        self.strength = strength
        self.typ = typ
        self.precision = precision
        self.normal = True
        if not normal:
            if strength <= 0.5:
                self.positive = False
            else:
                self.positive = True
            self.strength = 2.0*(strength-0.5)

    #def __str__(self):
        #return f"{self.text}/\t{self.strength}@\t{self.precision}"

    def __repr__(self):
        return f"{self.typ}(\ttext={self.text},\tpositive={self.positive},\tstrength={self.strength},\tnormal={self.normal},\tprecision={self.precision})"

class AdverbPhrase(Modifier):
    def __init__(self, text=None, positive=True, strength=0.5, normal=False, precision=0.5):
        Modifier.__init__(self, "AdverbPhrase", text, positive, strength, normal, precision)
        
    def to_adverb(self): return self

class AdjectivePhrase(Modifier):
    def __init__(self, text=None, positive=True, strength=0.5, normal=False, precision=0.5):
        Modifier.__init__(self, "AdjectivePhrase", text, positive, strength, normal, precision)
        
    def to_adverb(self):
        text = self.text
        ending = text[-1]
        if ending == "e":
            text = text[0:-1]+"ly"
        else:
            text = text+"ly"
        
        return AdverbPhrase(**self.locals(skip=["text", "typ"]), text=text)
    
class NounPhrase(Modifier):
    def __init__(self, text=None, positive=True, strength=0.5, normal=False, precision=0.5):
        Modifier.__init__(self, "NounPhrase", text, positive, strength, normal, precision)
        
    def to_adverb(self):
        return AdverbPhrase(**self.locals(skip=["text", "typ"]), text="with " + self.text)

class VerbPhrase(Modifier):
    def __init__(self, text=None, positive=True, strength=0.5, normal=False, precision=0.5):
        Modifier.__init__(self, "VerbPhrase", text, positive, strength, normal, precision)

    def to_adverb(self):
        return AdverbPhrase(**self.locals(skip=["text", "typ"]), text=self.text + " to")

def _adv(i: float, s: float, t: str):
    return AdverbPhrase   (text=t,strength=s, precision=(100.0-i)/100.0)
def _adj(i: float, s: float, t: str):
    return AdjectivePhrase(text=t,strength=s, precision=(100.0-i)/100.0)
def _nou(i: float, s: float, t: str):
    return NounPhrase     (text=t,strength=s, precision=(100.0-i)/100.0)
def _ver(i: float, s: float, t: str):
    return VerbPhrase     (text=t,strength=s, precision=(100.0-i)/100.0)

my_phrases_raw = [
    _adv(0.3,  0.99, "always"),
    _adj(1.1,  0.97, "certain"),
    _adv(5.5,  0.91, "almost always"),
    _nou(5.4,  0.91, "very high probability"),
    _adv(12.4, 0.87, "very often"),
    _adj(7.5,  0.86, "almost certain"),
    _adv(10.1, 0.85, "very likely"), # also adjective
    _adj(8.9,  0.85, "very probable"),
    _adj(14.5, 0.81, "very frequent"),
    _nou(10.1, 0.81, "high probability"),
    _nou(11.7, 0.81, "high chance"),
    _adv(16.7, 0.79, "usually"),     # poor precision
    _adv(15.0, 0.69, "likely"),      # also adjective
    _adj(13.0, 0.69, "probable"),
    _adv(10.4, 0.69, "often"),
    _adj(17.9, 0.66, "liable to happen"),
    _adj(15.2, 0.61, "frequent"),
    _adv(3.3,  0.61, "more often than not"),
    _nou(6.9,  0.58, "better than even chance"),
    _nou(18.5, 0.52, "moderate probability"),
    _adv(0.6,  0.50, "as often as not"),
    _nou(0.5,  0.50, "even chance"),
    _adj(24.6, 0.45, "not infrequent"),
    _nou(5.4,  0.41, "less than even chance"),
    _adj(7.9,  0.38, "less often than not"),
    _adj(42.7, 0.37, "possible"),
    _nou(29.1, 0.37, "not unreasonable"),
    _ver(30.2, 0.36, "might happen"),
    _adv(17.5, 0.26, "sometimes"),        # poor precision
    _adv(15.1, 0.23, "now and then"),     # poor precision
    _adv(15.2, 0.22, "occasionally"),     # poor precision
    _adv(16.3, 0.19, "unusually"),        # poor precision
    _adj(12.5, 0.17, "infrequent"),
    _adv(12.5, 0.17, "once in a while"),
    _nou(14.5, 0.16, "low probability"),
    _adv(13.0, 0.16, "unlikely"),        # adjective ?
    _adj(14.7, 0.15, "improbable"),
    _adv(14.5, 0.14, "not often"),
    _adv(14.3, 0.13, "not very often"),
    _nou(11.3, 0.13, "poor chance"),
    _nou(7.8,  0.13, "low chance"),
    _adv(10.1, 0.12, "seldom"),          # also adjective
    _adv(5.9,  0.08, "very unlikely"),    # adjective ? 
    _adj(6.4,  0.07, "very infrequent"),
    _adv(6.5,  0.07, "rarely"),
    _nou(5.7,  0.06, "very low probability"),
    _adj(5.9,  0.06, "very improbable"),
    _adv(4.5,  0.06, "very seldom"),
    _adv(3.8,  0.04, "very rarely"),
    _adv(3.4,  0.03, "almost never"),
    _adj(0.3,  0.01, "impossible"),
    _adv(0.3,  0.01, "never")
]

def closest_modifier(phrases, strength):
    return sorted(phrases, key=lambda w: abs(strength-w.strength))[0]

def best_modifier_in_range(phrases, lower, upper):
    ok = list(filter(lambda w: w.strength > lower and w.strength < upper, phrases))
    best = sorted(ok, key=lambda w: w.precision)
    if len(best) > 0: return best[-1]
    else: return None

def create_modifier_phrases(min_precision=0.0, resolution=0.1, types=lambda x: True):
    phrases_good = [w for w in my_phrases_raw 
                    if types(w) and w.precision >= min_precision]
    phrases_best = list(reversed(list(
        filter(
            lambda x: x is not None,
            [best_modifier_in_range(phrases_good, 
                f-resolution/2.0,
                f+resolution/2.0)
             for f in misc.frange(-1.0 - resolution, 1.0 + resolution, resolution)]))))

    return phrases_best