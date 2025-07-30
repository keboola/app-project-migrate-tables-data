<?php

declare(strict_types=1);

namespace Keboola\AppProjectMigrateLargeTables\Configuration;

use Keboola\Component\Config\BaseConfigDefinition;
use Symfony\Component\Config\Definition\Builder\ArrayNodeDefinition;
use Symfony\Component\Config\Definition\Exception\InvalidConfigurationException;

class CreateReplicationsConfigDefinition extends BaseConfigDefinition
{
    protected function getParametersDefinition(): ArrayNodeDefinition
    {
        $parametersNode = parent::getParametersDefinition();
        // @formatter:off
        /** @noinspection NullPointerExceptionInspection */
        $parametersNode
            ->children()
                ->scalarNode('sourceKbcUrl')->isRequired()->cannotBeEmpty()->end()
                ->scalarNode('sourceHost')->isRequired()->cannotBeEmpty()->end()
                ->scalarNode('sourceUsername')->isRequired()->cannotBeEmpty()->end()
                ->scalarNode('#sourcePassword')->cannotBeEmpty()->end()
                ->scalarNode('#sourcePrivateKey')->cannotBeEmpty()->end()
                ->integerNode('projectIdFrom')->isRequired()->end()
                ->integerNode('projectIdTo')->isRequired()->end()
            ->end()
            ->validate()->always(function ($v) {
                if (!empty($v['#privateKey']) && !empty($v['#password'])) {
                    throw new InvalidConfigurationException(
                        'You can use either privateKey or password, not both.',
                    );
                }
                if (empty($v['#privateKey']) && empty($v['#password'])) {
                    throw new InvalidConfigurationException(
                        'You must provide either privateKey or password.',
                    );
                }
                return $v;
            })->end()
        ;
        // @formatter:on
        return $parametersNode;
    }
}
