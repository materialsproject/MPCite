class DictAsMember(dict):
    # http://stackoverflow.com/questions/10761779/when-to-use-getattr/10761899#10761899
    def __getattr__(self, name):
        value = self[name]
        if isinstance(value, dict):
            value = DictAsMember(value)
        return value