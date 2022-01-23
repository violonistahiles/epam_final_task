class PathProcessor:

    @staticmethod
    def decode(path):
        pass

    @staticmethod
    def encode(path):
        pass

    @staticmethod
    def next_child_path(path, first=True):
        if first:
            return path + '.1'
        else:
            dot_index = path.rfind('.')
            previous_number = path[dot_index+1:]
            return path[:dot_index+1] + f'{(int(previous_number) + 1)}'

    @staticmethod
    def next_path(path='1', first=True):
        return path if first else str(int(path) + 1)

    @staticmethod
    def create_sorted_dict(comments, keys):
        result = dict()
        sorted_comments = sorted(comments,
                                 key=lambda x: (len(x[0].split('.')), x[0]))

        swap_dict = {}
        for ind, comment in enumerate(sorted_comments, start=1):
            if len(comment[0].split('.')) == 1:
                swap_dict.update({comment[0]: str(ind)})
            else:
                break

        for comment in sorted_comments:
            info = {key: value for key, value in zip(keys, comment[1:])}
            paths = comment[0].split('.')
            if len(paths) == 1:
                result[swap_dict[paths[0]]] = {**info, 'comments': dict()}
            else:
                tmp_dict = result[swap_dict[paths[0]]]['comments']
                for path in paths[1:-1]:
                    tmp_dict = tmp_dict[path]['comments']
                tmp_dict.update({paths[-1]: {**info, 'comments': dict()}})

        return result

    @staticmethod
    def create_dict(comments, keys):
        result = dict()
        for ind, comment in enumerate(comments, start=1):
            info = {key: value for key, value in zip(keys, comment[1:])}
            result.update({str(ind): info})

        return result
