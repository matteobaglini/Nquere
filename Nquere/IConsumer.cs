using System.Threading.Tasks;

namespace Nquere
{
    internal interface IConsumer
    {
        Task Task { get; }
        void Start();
        void Stop();
    }
}