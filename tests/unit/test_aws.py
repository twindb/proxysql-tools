from mock import MagicMock

from proxysql_tools.aws.aws import get_network_interface, \
    get_network_interface_state, network_interface_attached, \
    detach_network_interface


def test__get_network_interface(mocker):
    mock_boto3 = mocker.patch('proxysql_tools.aws.aws.boto3')
    mock_ec2 = MagicMock()
    mock_boto3.client.return_value = mock_ec2
    get_network_interface('0.0.0.0')
    mock_boto3.client.assert_called_once_with('ec2')


def test__get_network_interface_state(mocker):
    mock_boto3 = mocker.patch('proxysql_tools.aws.aws.boto3')
    mock_ec2 = MagicMock()
    mock_boto3.client.return_value = mock_ec2
    get_network_interface_state('some interface')
    mock_boto3.client.assert_called_once_with('ec2')


def test__network_interface_state_returns_true_if_attached(mocker):
    mock_get_interface_state = mocker.patch('proxysql_tools.aws.aws.get_network_interface_state')
    mock_get_interface_state.return_value = 'attached'
    assert network_interface_attached('1.1.1.1')


def test__network_interface_state_returns_false_if_not_attached(mocker):
    mock_get_interface_state = mocker.patch('proxysql_tools.aws.aws.get_network_interface_state')
    mock_get_interface_state.return_value = 'detached'
    assert not network_interface_attached('1.1.1.1')


def test__network_interface_attached_if_side_effect_is_key_error(mocker):
    mock_get_interface_state = mocker.patch('proxysql_tools.aws.aws.get_network_interface_state')
    mock_get_interface_state.side_effect = KeyError
    assert not network_interface_attached('1.1.1.1')


def test__detach_network_interface(mocker):
    mock_boto3 = mocker.patch('proxysql_tools.aws.aws.boto3')
    mock_ec2 = MagicMock()
    mock_boto3.client.return_value = mock_ec2
    detach_network_interface('stub')
    mock_boto3.client.assert_called_once_with('ec2')
