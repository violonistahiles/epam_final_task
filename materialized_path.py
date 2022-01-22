class PathProcessor:

    @staticmethod
    def get_next_child_path(path, first=True):
        if first:
            return path + '.1'
        else:
            dot_index = path.rfind('.')
            previous_number = path[dot_index+1:]
            return path[:dot_index+1] + f'{(int(previous_number) + 1)}'

    @staticmethod
    def get_next_path(path='1', first=True):
        return path if first else str(int(path) + 1)
