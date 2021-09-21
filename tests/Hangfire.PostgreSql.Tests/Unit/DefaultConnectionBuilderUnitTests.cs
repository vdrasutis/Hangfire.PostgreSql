using System;
using Hangfire.PostgreSql.Connectivity;
using Xunit;

namespace Hangfire.PostgreSql.Tests.Unit
{
    public class DefaultConnectionBuilderUnitTests
    {
        [Fact]
        public void Ctor_ThrowsAnException_WhenConnectionStringIsNull()
        {
            var exception = Assert.Throws<ArgumentNullException>(
                () => new DefaultConnectionBuilder(connectionString: null));

            Assert.Equal("connectionString", exception.ParamName);
        }

        [Fact]
        public void Ctor_ThrowsAnException_WhenConnectionStringIsInvalid()
        {
            var exception = Assert.Throws<ArgumentException>(
                () => new DefaultConnectionBuilder("testtest"));
            
            Assert.NotNull(exception);
        }

        [Fact]
        public void Ctor_ThrowsAnException_WhenConnectionStringBuilderIsNull()
        {
            var exception = Assert.Throws<ArgumentNullException>(
                () => new DefaultConnectionBuilder(connectionStringBuilder: null));

            Assert.Equal("connectionStringBuilder", exception.ParamName);
        }
    }
}
