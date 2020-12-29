def find_py4j_error_root_cause(java_exception):
    """
    Walks up the chain of Java exceptions by repeatedly calling getCause().

    :param java_exception: An instance of java_exception (Exception) from py4j.protocol.Py4JJavaError
    :return: The most nested Java Exception accessible
    """

    if java_exception:
        nested_exception = java_exception.getCause()

        if nested_exception:
            return find_py4j_error_root_cause(nested_exception)
        return java_exception


def is_py4j_error_root_cause_class(py4j_error, java_class_name):
    """
    Provides a 'best-effort' attempt at ascertaining the innermost nested exception in a Java Exception thrown by Py4j.
    Any errors caused by the attempt to introspect the error are swallowed so this method is safe to use within
    an except clause.

    :param py4j_error: An instance of py4j.protocol.Py4JJavaError
    :param java_class_name:
    :return:
    """

    if not py4j_error or not java_class_name:
        return False

    try:
        root_cause_error = find_py4j_error_root_cause(py4j_error.java_exception)
        if root_cause_error and root_cause_error.getClass().getName() == java_class_name:
            return True
    except:
        return False