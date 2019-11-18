import copy

root = None  # The logical root folder
client_wd = "/"  # What folder a client is checking at the moment
free_space = 10 ** 6


class Directory:
    """
    The class of directory node in the DFS directory tree.
    ---
    Attributes:
    - parent  : the directory current directory belongs to
    - subDirs : the list of subdirectories
    - files   : the list of files inside the directory
    - name    : the name of the directory
    """

    def __init__(self, name: str):
        self.name = name
        self.parent = None
        self.subDirs = []
        self.files = []

    def addFile(self, file_: "File") -> None:
        # Rewrite if already exists
        for f in self.files:
            if f.name == file_.name:
                self.files.remove(f)

        file_.directory = self
        self.files.append(file_)

    def addFiles(self, files) -> None:
        self.files = self.files + files

    def getFile(self, filename: str) -> "File":
        for f in self.files:
            if f.name == filename:
                return f
        return None

    def removeFile(self, filename: str) -> None:
        for f in self.files:
            if f.name == filename:
                self.files.remove(f)
                return None
        return None

    def addDir(self, dir_: "Directory") -> None:
        dir_.parent = self
        self.subDirs.append(dir_)

    def getSubDir(self, dirname: str) -> "Directory":
        if dirname[-1] != "/":
            dirname = dirname + "/"
        for d in self.subDirs:
            if d.name == dirname:
                return d
        return None

    def removeDir(self, dirname: str) -> None:
        for d in self.subDirs:
            if d.name == dirname:
                self.subDirs.remove(d)
                return
        print("There is no directory", dirname, "in", self.name, "!")
        return

    def getPath(self) -> str:
        """
        Returns the absolute location of a directory
        """
        path = []
        prnt = self.parent

        while prnt != None:
            path.append(prnt.name)
            prnt = prnt.parent
        path = path[::-1]

        result = ""
        for d in path:
            result += d

        result += self.name

        return result

    def printDirTree(self, offset=0) -> None:
        """
        Prints the directory files starting from current directory excluding files
        """
        print(" " * offset, self.name)
        for d in self.subDirs:
            d.printDirTree(offset + 2)

    def printFullTree(self, offset=0) -> None:
        """
        Prints the whole directory tree starting from current directory including files
        """
        print(" " * offset, "Dir  ", self.name)
        for f in self.files:
            print(" " * (offset + 2), "File ", f.name)
        for d in self.subDirs:
            d.printFullTree(offset + 2)

    def printInfo(self) -> None:
        """
        Prints information about directory filling
        """
        print("Directories:")
        for d in self.subDirs:
            print(d.name)
        print("Files:")
        for f in self.files:
            print(f.name)


class File:
    """
    The class of a file node in the DFS directory tree
    ---
    Attributes:
    - name      : name of the file
    - directory : a directory the file belongs to
    - nodes   : the nodes where this file is located physically
    """

    def __init__(self, name):
        self.name = name
        self.directory = None
        self.nodes = []
        self.size = 0

    def getPath(self) -> str:
        """
        Returns the absolute location of a file
        """
        path = []
        prnt = self.directory

        while prnt != None:
            path.append(prnt.name)
            prnt = prnt.parent
        path = path[::-1]

        result = ""
        for d in path:
            result += d

        result += self.name

        return result


def ListDiff(first, second):
    """
    Helper function
    """
    second = set(second)
    return [item for item in first if item not in second]


def GetFilesToReplicate(fallen, was_alive, is_root=True, dr=None) -> "list":
    """
    """

    alive = ListDiff(was_alive, fallen)

    files_to_replicate = []

    if is_root:
        for f in root.files:
            if f.nodes != None:
                for node in f.nodes:
                    if node not in alive:

                        f.nodes.remove(node)

                        node_receiver = ""
                        if len(f.nodes) != 0:
                            node_sender = f.nodes[0]
                        for rec_node in alive:
                            if rec_node not in f.nodes:
                                f.nodes.append(rec_node)
                                node_receiver = rec_node
                                break

                        files_to_replicate.append(
                            (f.getPath(), node_sender, node_receiver)
                        )
                        break

        for d in root.subDirs:
            files_to_replicate += GetFilesToReplicate(
                fallen, was_alive, is_root=False, dr=d
            )

    else:
        for f in dr.files:
            if f.nodes != None:
                for node in f.nodes:
                    if node not in alive:

                        f.nodes.remove(node)

                        node_receiver = ""
                        node_sender = ""
                        if len(f.nodes) != 0:
                            node_sender = f.nodes[0]
                        for rec_node in alive:
                            if rec_node not in f.nodes:
                                f.nodes.append(rec_node)
                                node_receiver = rec_node
                                break

                        files_to_replicate.append(
                            (f.getPath(), node_sender, node_receiver)
                        )
                        break

        for d in dr.subDirs:
            files_to_replicate += GetFilesToReplicate(
                fallen, was_alive, is_root=False, dr=d
            )

    return files_to_replicate


def GetAbsolutePath(some_path: str) -> "list of str":
    """
    Converts any path to an absolute path in list form
    If path contains file name, it will be in the last list element
    ---
    Attributes:
    - some_path: path in any of forms: '/abs_path/dir', 'rel_path/dir/', './rel_path/dir'
    """

    dirs = ["/"]

    if some_path == "/":
        return dirs

    if some_path == "./":
        if client_wd == "/":
            return dirs
        return dirs + client_wd[1:-1].split("/")

    if some_path[0] == "/":
        if some_path[-1] == "/":
            dirs = dirs + some_path[1:-1].split("/")
        else:
            dirs = dirs + some_path[1:].split("/")
    else:
        parent_dirs = []
        if client_wd != "/":
            parent_dirs = client_wd[1:-1].split("/")
        if some_path[:2] == "./":
            if some_path[-1] == "/":
                dirs = dirs + parent_dirs + some_path[2:-1].split("/")
            else:
                dirs = dirs + parent_dirs + some_path[2:].split("/")
        else:
            if some_path[-1] == "/":
                dirs = dirs + parent_dirs + some_path[:-1].split("/")
            else:
                dirs = dirs + parent_dirs + some_path.split("/")

    return dirs


def GetDir(path: str) -> bool:
    """
    Returns a directory with given path
    ---
    Attributes:
    - path: location of a directory
    """

    dirs = GetAbsolutePath(path)

    cd = root

    if len(dirs) == 1 and dirs[0] == "/":
        return root

    dirs = dirs[1:]

    for d in dirs:
        cd = cd.getSubDir(d)
        if cd == None:
            return None

    return cd


def DirExists(path: str) -> bool:
    """
    Returns whether the given directory exists or not
    ---
    Attributes:
    - path: location of a directory
    """

    return GetDir(path) != None


def GetFile(path: str) -> "File":
    """
    Returns specified file 
    """

    path = GetAbsolutePath(path)

    if len(path) == 2:
        # Root directory
        d = GetDir("/")
        f = d.getFile(path[1])
        if f != None:
            return f

        else:
            print("The file", "/" + path[1], "does not exists!")
            return None

    else:
        abs_path = "/"
        for d in path[1:-1]:
            abs_path += d + "/"

        filename = path[-1]

        dr = GetDir(abs_path)
        if dr != None:
            f = dr.getFile(filename)
            if f != None:
                return f

            else:
                print("The file", abs_path + filename, "does not exists!")
                return None

        else:
            print("The directory", abs_path, "does not exists!")
            return -1


def ChooseDataNodes(space_list: "list") -> "list":
    """
    Returns the list of data node IDs which have the most free space available
    """

    mx1 = 0
    mx2 = 0
    ind1 = 0
    ind2 = 0

    if space_list[0] > space_list[1]:
        mx1 = space_list[0]
        mx2 = space_list[1]
        ind1 = 0
        ind2 = 1

    else:
        mx1 = space_list[1]
        mx2 = space_list[0]
        ind1 = 1
        ind2 = 0

    for i in range(2, len(space_list)):
        if space_list[i] > mx1:
            mx2 = mx1
            ind2 = ind1
            mx1 = space_list[i]
            ind1 = i
        elif space_list[i] > mx2:
            mx2 = space_list[i]
            ind2 = i

    return [ind1, ind2]


def Initialize() -> None:
    """
    Used for initialization of the DFS
    Removes all previous data from the system
    """

    global root
    global client_wd

    root = Directory("/")  # Recreate the root eliminating all previous information

    client_wd = "/"

    # Wait for all data nodes to return their free space

    byte_num = 1024

    print("Free space:", byte_num, " bytes")
    return byte_num


def FileCreate(path: str, filesize=0, nodes=None, empty=True) -> str:
    """
    Creates a logical file
    ---
    Attributes:
    - path     : the location of a new file
    - nodes    : the IDs of the nodes on which the file will be stored
    - empty    : if true performs 'File create' command, performs 'File write' otherwise 
    ---
    Returns:
    Absolute DFS path if OK, None if not OK
    """
    global free_space

    if filesize != 0:
        if filesize > free_space:
            print("Too big file!")
            return None

    free_space -= filesize

    abs_path = GetAbsolutePath(path)

    filename = abs_path[-1]
    path = "/"
    for d in abs_path[1:-1]:
        path += d + "/"

    for ch in filename:
        if ch == "/":
            print("A filename should not contain '/' character!")
            return None

    dest = GetDir(path)

    if dest != None:
        f = File(filename)
        f.nodes = nodes
        f.size = filesize
        dest.addFile(f)
        print(free_space, f.size)
        return f.getPath()

    else:
        print("The directory", path, "does not exists!")
        return None


def FileWrite(path: str, nodes=None, filesize=0) -> str:
    """
    Returns:
    Absolute DFS path if OK, None if not OK
    """

    return FileCreate(path, nodes=nodes, filesize=filesize, empty=False)


def FileRead(path: str) -> str:
    """
    Returns:
    Absolute DFS path to the file if OK, None if not OK
    """

    f = GetFile(path)

    if f == None:
        return None

    return f.getPath(), f.nodes[0]


def FileDelete(path: str) -> str:
    """
    Returns:
    Absolute DFS path of deleted file if OK, None if not OK
    """

    global free_space

    f = GetFile(path)

    if f == None:
        return None

    res_path = f.getPath()
    free_space += f.size
    f.directory.removeFile(f.name)

    return res_path


def FileInfo(path: str) -> str:
    """
    """

    f = GetFile(path)

    if f == None:
        return None

    info = ""

    info += "Name: " + f.name + "\n"
    info += "Path: " + f.getPath() + "\n"
    info += "Replicas: " + str(f.nodes) + "\n"
    info += "Size: (bytes)" + str(f.size) + "\n"

    return info


def FileCopy(path: str, dest: str) -> str:
    """
    Returns:
    Absolute DFS path to the file to be copied, absolute path of the destination directory if OK
    Or None if not OK
    """

    f = GetFile(path)

    if f == None:
        print("FILEERROR", f)
        return None

    d = GetDir(dest)

    if d == None:
        print("DIRERROR", f)
        return None

    if d == f.directory:
        # print("ERROew", f)
        print("Cannot place copy into one directory with the original file!")
        return None

    f_copy = copy.deepcopy(f)

    d.addFile(f_copy)

    return f.getPath(), d.getPath()


def FileMove(path: str, dest: str) -> str:
    """
    Returns:
    Absolute DFS path to the file to be moved, absolute path of the destination directory if OK
    Or None if not OK
    """

    f = GetFile(path)

    if f == None:
        return None

    d = GetDir(dest)

    if d == None:
        return None

    res_path = f.getPath()

    f.directory.removeFile(f.name)
    d.addFile(f)

    return res_path, d.getPath()


def DirCreate(path: str) -> str:
    """
    Creates a new directory
    ---
    Attributes:
    - path: a location of new directory
    ---
    Returns:
    Absolute DFS path of a new directory if OK, None if not OK
    """

    if DirExists(path):
        print("The directory", path, "already exists!")
        return None

    else:
        dirs = GetAbsolutePath(path)
        dirname = dirs[-1]
        dirs = dirs[:-1]

        if len(dirs) == 1 and dirs[0] == "/":
            root.addDir(Directory(dirname + "/"))

            return root.getSubDir(dirname + "/").getPath()

        dirs = dirs[1:]

        cd = root
        for d in dirs:
            cd = cd.getSubDir(d)
            if cd == None:
                print("The path", path, "is not reachable!")
                return None

        cd.addDir(Directory(dirname + "/"))

        return cd.getSubDir(dirname + "/").getPath()


def DirOpen(path: str) -> str:
    """
    Changes working directory to a new value
    ---
    Attributes:
    - path: location of new working directory
    ---
    Returns:
    1 if OK, None if not OK
    """

    global client_wd

    if DirExists(path):
        abs_path = GetAbsolutePath(path)[1:]
        client_wd = "/"
        for d in abs_path:
            client_wd += d + "/"
        return 1
    else:
        print("The directory", path, "does not exists!")
        return None


def DirRead(path: str) -> str:
    """
    Returns list of files and subdirectories stored in a given directory
    ---
    Attributes:
    - path: location of desired directory
    """

    dr = GetDir(path)

    if dr == None:
        return None

    dir_abs_path = ''.join(GetAbsolutePath(path))
    info = ""

    info += "CWD: {} \nDirectory name: {} \n".format(client_wd, dir_abs_path)

    info += "Files:\n"
    for f in dr.files:
        info += " " * 2 + f.name + "    "
        info += " " * 2 + str(f.nodes) + "\n"
    info += "Subdirectories:\n"
    for d in dr.subDirs:
        info += " " * 2 + d.name + "\n"

    return info


def DirDelete(path: str) -> str:
    """
    Returns:
    Absolute DFS path of the directory to be deleted if OK, None if not OK
    """

    global client_wd

    if path == "/":
        print("You cannot delete root directory! Use 'Initialization' command instead.")
        return None

    dr = GetDir(path)

    if dr == None:
        return None

    # Add free space
    FreeSpace(dr)

    res_path = dr.getPath()
    parent = dr.parent
    parent.removeDir(dr.name)

    if GetDir(client_wd) == None:
        client_wd = parent.getPath()

    return res_path


def IsEmpty(path: str) -> bool:
    """
    Returns True if given directory contains files and False otherwise
    """

    d = GetDir(path)

    if d == None:
        print("The directory", d.getPath(), "does not exists!")
        return None

    if len(d.files) > 0:
        return False

    is_subs_empty = True
    for sd in d.subDirs:
        is_subs_empty = is_subs_empty & IsEmpty(sd.getPath())

    return is_subs_empty


def FreeSpace(dr: "Directory") -> None:
    """
    """

    global free_space

    for f in dr.files:
        free_space += f.size

    for d in dr.subDirs:
        FreeSpace(d)

    return


def main():
    Initialize()
    print()

    root.printFullTree()
    print()

    print(DirCreate("mashinka"))
    print(DirCreate("pictures"))
    print(FileCreate("report.pdf", nodes=["1", "2"]))

    print(DirOpen("mashinka"))
    print(FileCreate("file.txt", nodes=["1", "2"]))
    print(DirCreate("project"))

    print(DirOpen("./project"))
    print(DirCreate("baseline_model"))
    print(DirCreate("final_model"))
    print(DirCreate("final_model/some_dir"))
    print(FileCreate("report2.txt", nodes=["1", "2"]))

    print(DirOpen("/pictures/"))

    print('MY CHANGE\n\n\n')
    print(DirRead("./"))



    print(FileWrite("pikcha1.png", filesize=500, nodes=["1", "2"]))
    print(FileCreate("pikcha2.png", nodes=["1", "2"]))
    print(FileCopy("pikcha2.png", "./"))
    print(FileMove("pikcha1.png", "/mashinka/project/baseline_model"))

    print(FileRead("pikcha2.png"))

    print(IsEmpty("/mashinka/project/final_model"))

    print(ChooseDataNodes([15, 50, 30, 340, 2800, 20000]))

    print(free_space)
    print(FileWrite("someFile.dat", filesize=500, nodes=["1", "2"]))
    print(FileDelete("someFile.dat"))

    print(DirDelete("/mashinka"))
    print(DirDelete("/pictures"))

    print("Client wd", client_wd)

    print(DirCreate("/dir1"))
    print(DirCreate("/dir1/dir2"))
    print(DirCreate("/dir1/dir2/dir3"))
    print(DirCreate("/dir1/dir2/dir3/dir4"))

    print(FileDelete("report.pdf"))

    print(FileWrite("a.dat", nodes=["1", "2"], filesize=100))

    print(GetFilesToReplicate(fallen=["1"], was_alive=["1", "2", "3"]))

    print(DirDelete("dir1"))

    print(free_space)
    print()

    print()
    root.printFullTree()
    print()


if __name__ == "__main__":
    main()
